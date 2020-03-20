package com.github.panghy;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.errors.ApiException;
import com.google.maps.internal.ratelimiter.RateLimiter;
import com.google.maps.model.AddressComponent;
import com.google.maps.model.AddressComponentType;
import com.google.maps.model.GeocodingResult;
import com.google.maps.model.LatLng;
import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.direct.ingestion.WavefrontDirectIngestionClient;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.StringReader;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class CSSEDISandDataLoader {

  private static final Logger log = Logger.getLogger(CSSEDISandDataLoader.class.getCanonicalName());

  @Parameter(names = "--wavefrontUrl")
  public String wavefrontUrl = "https://longboard.wavefront.com";

  @Parameter(names = "--wavefrontToken")
  public String wavefrontToken;

  @Parameter(names = "--googleGeolocationApiToken")
  public String googleGeolocationApiToken;

  @Parameter(names = "--flushPPS")
  public double flushPPS = 10000.0;

  private static final DateTimeFormatter MONTH_DAY_YEAR = DateTimeFormatter.ofPattern("M/d/[uuuu][uu]");
  private static final DateTimeFormatter MONTH_DAY_HOUR_MINUTE = new DateTimeFormatterBuilder()
      .appendPattern("M/d HH:mm")
      .parseDefaulting(ChronoField.YEAR, 2020)
      .toFormatter(Locale.ENGLISH);
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final ZoneId GENEVA = ZoneId.of("GMT+1");
  private static final ZoneId ET = ZoneId.of("America/New_York");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<Pair<Double, Double>, AddressComponent[]> cachedReverseGeocodingResults = new HashMap<>();
  private final Map<String, GeocodingResult[]> cachedGeocodingResults = new HashMap<>();

  public static void main(String[] args) throws Exception {
    CSSEDISandDataLoader dataLoader = new CSSEDISandDataLoader();
    new JCommander(dataLoader, null, args);
    dataLoader.run();
  }

  public void run() throws Exception {
    OkHttpClient httpClient = new OkHttpClient.Builder().
        callTimeout(10, TimeUnit.SECONDS).
        connectTimeout(10, TimeUnit.SECONDS).
        hostnameVerifier((hostname, session) -> true).
        build();
    GeoApiContext geoApiContext = new GeoApiContext.Builder()
        .apiKey(googleGeolocationApiToken)
        .build();
    WavefrontSender wavefrontSender = new WavefrontDirectIngestionClient.Builder(wavefrontUrl, wavefrontToken).build();
    RateLimiter ppsRateLimiter = RateLimiter.create(flushPPS);
    while (true) {
      long start = System.currentTimeMillis();
      try {
        covidTrackingUSStatesDataHistorical(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter);
        covidTrackingUSStatesData(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter);
        fetchAndProcessCSSEData(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter,
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv",
            "confirmed_cases");
        fetchAndProcessCSSEData(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter,
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv",
            "deaths");
        fetchAndProcessCSSEData(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter,
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv",
            "recovered");
        fetchAndProcessWHOData(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter,
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/who_covid_19_situation_reports/who_covid_19_sit_rep_time_series/who_covid_19_sit_rep_time_series.csv",
            "confirmed");
      } catch (Exception ex) {
        log.log(Level.SEVERE, "Uncaught exception in run() loop", ex);
      }
      log.info("Cycle Complete [" + (System.currentTimeMillis() - start) + "ms]");
      Thread.sleep(TimeUnit.MINUTES.toMillis(5));
    }
  }

  private void fetchAndProcessCSSEData(OkHttpClient httpClient, GeoApiContext geoApiContext,
                                       WavefrontSender wavefrontSender, RateLimiter ppsRateLimiter,
                                       String url, String context) {
    try (Response response = httpClient.newCall(
        new Request.Builder().url(url).get().build()).
        execute()) {
      if (response.code() != 200 || response.body() == null) {
        log.log(Level.WARNING, "Cannot fetch confirmed cases data");
      } else {
        String body = response.body().string();
        CSVParser records = CSVFormat.RFC4180.withFirstRecordAsHeader().withAllowMissingColumnNames().
            parse(new StringReader(body));
        List<String> headerNames = records.getHeaderNames();
        // Column 1 is Province/State, Column 2 is Country/Region, Column 3 is Lat and Column 4 is Long, then dates.
        Map<Integer, LocalDate> columnOrdinalToDates = new HashMap<>();
        for (int i = 4; i < headerNames.size(); i++) {
          String dateHeader = headerNames.get(i);
          if (!StringUtils.isBlank(dateHeader)) {
            LocalDate date = LocalDate.parse(dateHeader, MONTH_DAY_YEAR);
            columnOrdinalToDates.put(i, date);
          }
        }
        for (CSVRecord csvRecord : records) {
          String provinceState = csvRecord.get(0);
          String countryRegion = csvRecord.get(1);
          double latitude = Double.parseDouble(csvRecord.get(2));
          double longitude = Double.parseDouble(csvRecord.get(3));
          AddressComponent[] addressComponents =
              cachedReverseGeocodingResults.computeIfAbsent(new Pair<>(latitude, longitude), latlong -> {
                GeocodingResult[] geocodingResults;
                try {
                  geocodingResults = GeocodingApi.reverseGeocode(geoApiContext,
                      new LatLng(latitude, longitude)).await();
                } catch (ApiException | IOException | InterruptedException e) {
                  throw new RuntimeException(e);
                }
                if (geocodingResults.length == 0) {
                  return new AddressComponent[0];
                } else {
                  return geocodingResults[0].addressComponents;
                }
              });
          Map<String, String> tags = new HashMap<>();
          if (!StringUtils.isBlank(countryRegion)) {
            tags.put("csse_country", countryRegion);
            Optional<AddressComponent> geocodedCountry = Arrays.stream(addressComponents).
                filter(ac -> Arrays.stream(ac.types).anyMatch(act -> act == AddressComponentType.COUNTRY)).
                findFirst();
            if (geocodedCountry.isPresent()) {
              if (StringUtils.isBlank(geocodedCountry.get().longName)) {
                tags.put("country", countryRegion);
              } else {
                tags.put("country", geocodedCountry.get().longName);
              }
              if (!StringUtils.isBlank(geocodedCountry.get().shortName)) {
                tags.put("country_short", geocodedCountry.get().shortName);
              }
            } else {
              tags.put("country", countryRegion);
            }
          }
          if (!StringUtils.isBlank(provinceState)) {
            tags.put("csse_province", provinceState);
            Optional<AddressComponent> geocodedProvince = Arrays.stream(addressComponents).
                filter(ac -> Arrays.stream(ac.types).anyMatch(act ->
                    act == AddressComponentType.ADMINISTRATIVE_AREA_LEVEL_1)).
                findFirst();
            if (geocodedProvince.isPresent()) {
              if (StringUtils.isBlank(geocodedProvince.get().longName)) {
                tags.put("province_state", provinceState);
              } else {
                tags.put("province_state", geocodedProvince.get().longName);
              }
              if (isNotBlank(geocodedProvince.get().shortName)) {
                tags.put("province_state_short", geocodedProvince.get().shortName);
              }
            } else {
              tags.put("province_state", provinceState);
            }
          }
          tags.put("version", "2");
          tags.put("lat", String.valueOf(latitude));
          tags.put("long", String.valueOf(longitude));
          for (int i = 4; i < csvRecord.size(); i++) {
            if (columnOrdinalToDates.containsKey(i) && !StringUtils.isBlank(csvRecord.get(i))) {
              int count = Integer.parseInt(csvRecord.get(i));
              LocalDate date = columnOrdinalToDates.get(i);
              ZonedDateTime utcDateTime = date.atStartOfDay(UTC);
              ppsRateLimiter.acquire();
              wavefrontSender.sendMetric("csse." + context, count, utcDateTime.toEpochSecond(), "csse", tags);
            }
          }
        }
      }
    } catch (Exception ex) {
      log.log(Level.SEVERE, "Uncaught exception when processing CSSE data", ex);
    }
  }

  private void fetchAndProcessWHOData(OkHttpClient httpClient, GeoApiContext geoApiContext,
                                      WavefrontSender wavefrontSender, RateLimiter ppsRateLimiter,
                                      String url, String context) {
    try (Response response = httpClient.newCall(
        new Request.Builder().url(url).get().build()).
        execute()) {
      if (response.code() != 200 || response.body() == null) {
        log.log(Level.WARNING, "Cannot fetch confirmed cases data");
      } else {
        String body = response.body().string();
        CSVParser records = CSVFormat.RFC4180.withFirstRecordAsHeader().withAllowMissingColumnNames().
            parse(new StringReader(body));
        List<String> headerNames = records.getHeaderNames();
        // Column 1 is Province/State, Column 2 is Country/Region, Column 3 is WHO region, then dates.
        Map<Integer, LocalDate> columnOrdinalToDates = new HashMap<>();
        for (int i = 3; i < headerNames.size(); i++) {
          String dateHeader = headerNames.get(i);
          if (!StringUtils.isBlank(dateHeader)) {
            LocalDate date = LocalDate.parse(dateHeader, MONTH_DAY_YEAR);
            columnOrdinalToDates.put(i, date);
          }
        }
        for (CSVRecord csvRecord : records) {
          String provinceState = csvRecord.get(0);
          String countryRegion = csvRecord.get(1);
          String whoRegion = csvRecord.get(2);
          String geocodingInput = StringUtils.isBlank(provinceState) ? "" : provinceState + ", ";
          geocodingInput += countryRegion;
          if (!StringUtils.isBlank(whoRegion)) {
            geocodingInput += ", " + whoRegion;
          }
          String finalGeocodingInput = geocodingInput;
          GeocodingResult[] geocodingResult = cachedGeocodingResults.computeIfAbsent(geocodingInput, key -> {
            try {
              return GeocodingApi.geocode(geoApiContext, finalGeocodingInput).await();
            } catch (ApiException | InterruptedException | IOException e) {
              throw new RuntimeException(e);
            }
          });
          Map<String, String> tags = new HashMap<>();
          if (geocodingResult.length > 0) {
            GeocodingResult firstResult = geocodingResult[0];
            if (firstResult.geometry != null) {
              LatLng location = firstResult.geometry.location;
              if (location != null) {
                tags.put("lat", String.valueOf(location.lat));
                tags.put("long", String.valueOf(location.lng));
              }
            }
            if (firstResult.addressComponents != null && firstResult.addressComponents.length > 0) {
              Optional<AddressComponent> geocodedCountry = Arrays.stream(firstResult.addressComponents).
                  filter(ac -> Arrays.stream(ac.types).anyMatch(act -> act == AddressComponentType.COUNTRY)).
                  findFirst();
              if (geocodedCountry.isPresent()) {
                if (isNotBlank(geocodedCountry.get().longName)) {
                  tags.put("country", geocodedCountry.get().longName);
                }
                if (isNotBlank(geocodedCountry.get().shortName)) {
                  tags.put("country_short", geocodedCountry.get().shortName);
                }
              }
            }
            if (firstResult.addressComponents != null && firstResult.addressComponents.length > 0) {
              Optional<AddressComponent> geocodedProvince = Arrays.stream(firstResult.addressComponents).
                  filter(ac -> Arrays.stream(ac.types).anyMatch(act -> act == AddressComponentType.ADMINISTRATIVE_AREA_LEVEL_1)).
                  findFirst();
              if (geocodedProvince.isPresent()) {
                if (isNotBlank(geocodedProvince.get().longName)) {
                  tags.put("province_state", geocodedProvince.get().longName);
                }
                if (isNotBlank(geocodedProvince.get().shortName)) {
                  tags.put("province_state_short", geocodedProvince.get().shortName);
                }
              }
            }
          }
          if (!StringUtils.isBlank(countryRegion)) {
            tags.put("who_country", countryRegion);
          }
          if (!StringUtils.isBlank(provinceState)) {
            tags.put("who_province_state", provinceState);
          }
          if (!StringUtils.isBlank(whoRegion)) {
            tags.put("who_region", whoRegion);
          }
          tags.put("version", "3");
          for (int i = 3; i < csvRecord.size(); i++) {
            if (columnOrdinalToDates.containsKey(i) && !StringUtils.isBlank(csvRecord.get(i))) {
              int count = Integer.parseInt(csvRecord.get(i));
              LocalDate date = columnOrdinalToDates.get(i);
              ZonedDateTime utcDateTime = date.atStartOfDay(GENEVA);
              ppsRateLimiter.acquire();
              wavefrontSender.sendMetric("who." + context, count, utcDateTime.toEpochSecond(), "csse", tags);
            }
          }
        }
      }
    } catch (Exception ex) {
      log.log(Level.SEVERE, "Uncaught exception when processing CSSE data", ex);
    }
  }

  private void covidTrackingUSStatesData(OkHttpClient httpClient, GeoApiContext geoApiContext,
                                         WavefrontSender wavefrontSender, RateLimiter ppsRateLimiter) {
    try (Response response = httpClient.
        newCall(new Request.Builder().url("https://covidtracking.com/api/states").get().build()).execute()) {
      if (response.code() == 200 && response.body() != null) {
        JsonNode data = OBJECT_MAPPER.readTree(response.body().string());
        for (JsonNode stateNode : data) {
          String stateCode = stateNode.get("state").asText();
          long positive = jsonNodeToNumber(stateNode.get("positive"));
          long negative = jsonNodeToNumber(stateNode.get("negative"));
          long pending = jsonNodeToNumber(stateNode.get("pending"));
          long death = jsonNodeToNumber(stateNode.get("death"));
          long total = jsonNodeToNumber(stateNode.get("total"));
          ZonedDateTime checkTimeEt = LocalDateTime.parse(stateNode.get("checkTimeEt").asText(), MONTH_DAY_HOUR_MINUTE).
              with(ChronoField.YEAR, 2020).atZone(ET);
          GeocodingResult[] geocodingResult = geocodeStateCode(geoApiContext, stateCode);
          Map<String, String> tags = new HashMap<>();
          tags.put("state_short", stateCode);
          if (geocodingResult.length > 0) {
            GeocodingResult firstResult = geocodingResult[0];
            if (firstResult.geometry != null) {
              LatLng location = firstResult.geometry.location;
              if (location != null) {
                tags.put("lat", String.valueOf(location.lat));
                tags.put("long", String.valueOf(location.lng));
              }
            }
            if (firstResult.addressComponents != null && firstResult.addressComponents.length > 0) {
              Optional<AddressComponent> geocodedCountry = Arrays.stream(firstResult.addressComponents).
                  filter(ac -> Arrays.stream(ac.types).anyMatch(act -> act == AddressComponentType.COUNTRY)).
                  findFirst();
              if (geocodedCountry.isPresent()) {
                if (isNotBlank(geocodedCountry.get().longName)) {
                  tags.put("country", geocodedCountry.get().longName);
                }
                if (isNotBlank(geocodedCountry.get().shortName)) {
                  tags.put("country_short", geocodedCountry.get().shortName);
                }
              }
            }
            if (firstResult.addressComponents != null && firstResult.addressComponents.length > 0) {
              Optional<AddressComponent> geocodedProvince = Arrays.stream(firstResult.addressComponents).
                  filter(ac -> Arrays.stream(ac.types).anyMatch(act -> act ==
                      AddressComponentType.ADMINISTRATIVE_AREA_LEVEL_1)).
                  findFirst();
              if (geocodedProvince.isPresent()) {
                if (isNotBlank(geocodedProvince.get().longName)) {
                  tags.put("state", geocodedProvince.get().longName);
                }
              }
            }
            tags.put("version", "1");
            long timestamp = checkTimeEt.toEpochSecond();
            ppsRateLimiter.acquire(5);
            wavefrontSender.sendMetric("com.covidtracking.positive", positive, timestamp, "covidtracking.com", tags);
            wavefrontSender.sendMetric("com.covidtracking.negative", negative, timestamp, "covidtracking.com", tags);
            wavefrontSender.sendMetric("com.covidtracking.pending", pending, timestamp, "covidtracking.com", tags);
            wavefrontSender.sendMetric("com.covidtracking.death", death, timestamp, "covidtracking.com", tags);
            wavefrontSender.sendMetric("com.covidtracking.total", total, timestamp, "covidtracking.com", tags);
          }
        }
      }
    } catch (Exception ex) {
      log.log(Level.SEVERE, "Uncaught exception when processing covidtracking.com states data", ex);
    }
  }

  private void covidTrackingUSStatesDataHistorical(OkHttpClient httpClient, GeoApiContext geoApiContext,
                                                   WavefrontSender wavefrontSender, RateLimiter ppsRateLimiter) {
    try (Response response = httpClient.
        newCall(new Request.Builder().url("https://covidtracking.com/api/states/daily").get().build()).execute()) {
      if (response.code() == 200 && response.body() != null) {
        JsonNode data = OBJECT_MAPPER.readTree(response.body().string());
        for (JsonNode stateNode : data) {
          String stateCode = stateNode.get("state").asText();
          long positive = jsonNodeToNumber(stateNode.get("positive"));
          long negative = jsonNodeToNumber(stateNode.get("negative"));
          long pending = jsonNodeToNumber(stateNode.get("pending"));
          long death = jsonNodeToNumber(stateNode.get("death"));
          long total = jsonNodeToNumber(stateNode.get("total"));
          LocalDateTime dateChecked = LocalDateTime.parse(stateNode.get("dateChecked").asText(),
              DateTimeFormatter.ISO_OFFSET_DATE_TIME);
          GeocodingResult[] geocodingResult = geocodeStateCode(geoApiContext, stateCode);
          Map<String, String> tags = new HashMap<>();
          tags.put("state_short", stateCode);
          if (geocodingResult.length > 0) {
            GeocodingResult firstResult = geocodingResult[0];
            if (firstResult.geometry != null) {
              LatLng location = firstResult.geometry.location;
              if (location != null) {
                tags.put("lat", String.valueOf(location.lat));
                tags.put("long", String.valueOf(location.lng));
              }
            }
            if (firstResult.addressComponents != null && firstResult.addressComponents.length > 0) {
              Optional<AddressComponent> geocodedCountry = Arrays.stream(firstResult.addressComponents).
                  filter(ac -> Arrays.stream(ac.types).anyMatch(act -> act == AddressComponentType.COUNTRY)).
                  findFirst();
              if (geocodedCountry.isPresent()) {
                if (isNotBlank(geocodedCountry.get().longName)) {
                  tags.put("country", geocodedCountry.get().longName);
                }
                if (isNotBlank(geocodedCountry.get().shortName)) {
                  tags.put("country_short", geocodedCountry.get().shortName);
                }
              }
            }
            if (firstResult.addressComponents != null && firstResult.addressComponents.length > 0) {
              Optional<AddressComponent> geocodedProvince = Arrays.stream(firstResult.addressComponents).
                  filter(ac -> Arrays.stream(ac.types).anyMatch(act -> act ==
                      AddressComponentType.ADMINISTRATIVE_AREA_LEVEL_1)).
                  findFirst();
              if (geocodedProvince.isPresent()) {
                if (isNotBlank(geocodedProvince.get().longName)) {
                  tags.put("state", geocodedProvince.get().longName);
                }
              }
            }
            tags.put("version", "1");
            long timestamp = dateChecked.toEpochSecond(ZoneOffset.UTC);
            ppsRateLimiter.acquire(5);
            wavefrontSender.sendMetric("com.covidtracking.positive", positive, timestamp,
                "covidtracking.com", tags);
            wavefrontSender.sendMetric("com.covidtracking.negative", negative, timestamp,
                "covidtracking.com", tags);
            wavefrontSender.sendMetric("com.covidtracking.pending", pending, timestamp,
                "covidtracking.com", tags);
            wavefrontSender.sendMetric("com.covidtracking.death", death, timestamp,
                "covidtracking.com", tags);
            wavefrontSender.sendMetric("com.covidtracking.total", total, timestamp,
                "covidtracking.com", tags);
          }
        }
      }
    } catch (Exception ex) {
      log.log(Level.SEVERE, "Uncaught exception when processing covidtracking.com states data", ex);
    }
  }

  private GeocodingResult[] geocodeStateCode(GeoApiContext geoApiContext, String stateCode) {
    String geocodeInput = stateCode;
    switch (stateCode) {
      case "AS":
        geocodeInput = "American Samoa";
        break;
      case "GU":
        geocodeInput = "Guam";
        break;
      case "MP":
        geocodeInput = "Northern Mariana Islands";
        break;
      case "PR":
        geocodeInput = "Puerto Rico";
        break;
      case "VI":
        geocodeInput = "US Virgin Islands";
        break;
    }
    return cachedGeocodingResults.computeIfAbsent(geocodeInput + ", US",
        key -> {
          try {
            return GeocodingApi.geocode(geoApiContext, key).await();
          } catch (ApiException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private long jsonNodeToNumber(JsonNode node) {
    if (node.isNull()) return 0;
    return Long.parseLong(node.asText());
  }

  private String iso2CountryCodeToIso3CountryCode(String iso2CountryCode) {
    Locale locale = new Locale("", iso2CountryCode);
    return locale.getISO3Country();
  }
}
