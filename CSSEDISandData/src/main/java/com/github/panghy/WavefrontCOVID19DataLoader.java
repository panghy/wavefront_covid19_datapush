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
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.direct.ingestion.WavefrontDirectIngestionClient;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.Optional.empty;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class WavefrontCOVID19DataLoader {

  private static final Logger log = Logger.getLogger(WavefrontCOVID19DataLoader.class.getCanonicalName());

  @Parameter(names = "--wavefrontUrl")
  public String wavefrontUrl = "https://longboard.wavefront.com";

  @Parameter(names = "--wavefrontToken")
  public String wavefrontToken;

  @Parameter(names = "--googleGeolocationApiToken")
  public String googleGeolocationApiToken;

  @Parameter(names = "--flushPPS")
  public double flushPPS = 10000.0;

  @Parameter(names = "--historical")
  public boolean historical = false;

  @Parameter(names = "--cachedGeocodingResults")
  public String cachedGeocodingResultsFile = "geocoding_results.bin";

  @Parameter(names = "--cachedReverseGeocodingResults")
  public String cachedReverseGeocodingResultsFile = "reverse_geocoding_results.bin";

  private static final DateTimeFormatter MONTH_DAY_YEAR = DateTimeFormatter.ofPattern("M/d/[uuuu][uu]");
  private static final DateTimeFormatter MONTH_DAY_HOUR_MINUTE = new DateTimeFormatterBuilder()
      .appendPattern("M/d HH:mm")
      .parseDefaulting(ChronoField.YEAR, 2020)
      .toFormatter(Locale.ENGLISH);
  private static final DateTimeFormatter JHU_TS_V2 = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
  private static final DateTimeFormatter JHU_TS_V2_1 = DateTimeFormatter.ofPattern("M/d/uu H:mm[:ss]");
  private static final DateTimeFormatter JHU_DAILY_FILE_FORMAT = DateTimeFormatter.ofPattern("MM-dd-uuuu");

  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final ZoneId GENEVA = ZoneId.of("GMT+1");
  private static final ZoneId ET = ZoneId.of("America/New_York");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<Pair<Double, Double>, AddressComponent[]> cachedReverseGeocodingResults = new HashMap<>();
  private final Map<String, GeocodingResult[]> cachedGeocodingResults = new HashMap<>();

  public static void main(String[] args) throws Exception {
    WavefrontCOVID19DataLoader dataLoader = new WavefrontCOVID19DataLoader();
    new JCommander(dataLoader, null, args);
    dataLoader.run();
  }

  public void run() throws Exception {
    // deserialize cached geocoding results.
    loadCachedGeocodingResults();
    // initialize clients
    OkHttpClient httpClient = new OkHttpClient.Builder().
        callTimeout(10, TimeUnit.SECONDS).
        connectTimeout(10, TimeUnit.SECONDS).
        hostnameVerifier((hostname, session) -> true).
        build();
    GeoApiContext geoApiContext = new GeoApiContext.Builder()
        .apiKey(googleGeolocationApiToken)
        .build();
    WavefrontSender wavefrontSender = new WavefrontDirectIngestionClient.Builder(wavefrontUrl, wavefrontToken).
        maxQueueSize(1_000_000).build();
    RateLimiter ppsRateLimiter = RateLimiter.create(flushPPS);
    // used during testing.
    String metricPrefix = "";
    String csseDataVersion = "v10";
    while (true) {
      long start = System.currentTimeMillis();
      try {
        // VMware FAH data
        log.info("Processing VMware FAH Data: 52737");
        fetchFAHData(wavefrontSender, ppsRateLimiter, httpClient, "52737");
        // JHU daily data v2
        LocalDate csvFileDate = LocalDate.of(2020, 3, 22);
        LocalDate today = LocalDate.now(UTC);
        while (!csvFileDate.isAfter(today)) {
          String file = JHU_DAILY_FILE_FORMAT.format(csvFileDate) + ".csv";
          log.info("Processing JHU Daily: " + file);
          fetchAndProcessDailyCSSEData(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter,
              "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/" + file,
              metricPrefix, csseDataVersion);
          csvFileDate = csvFileDate.plusDays(1);
        }
        log.info("Processing JHU Aggregated (until 3/22/2020)");
        // JHU CSSE data until 3/22 (3/23 switches to the new data).
        fetchAndProcessCSSEData(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter,
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/203881b83c3820521f5af7cafb0d15367e415652/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv",
            "confirmed_cases", metricPrefix, csseDataVersion);
        fetchAndProcessCSSEData(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter,
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/203881b83c3820521f5af7cafb0d15367e415652/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv",
            "deaths", metricPrefix, csseDataVersion);
        fetchAndProcessCSSEData(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter,
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/203881b83c3820521f5af7cafb0d15367e415652/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv",
            "recovered", metricPrefix, csseDataVersion);
        log.info("Processing worldometer.info stats");
        // worldometer.info world stats
        injectCountryStats(geoApiContext, wavefrontSender, ppsRateLimiter, historical);
        // only need to run this once to get historical data.
        if (historical) {
          log.info("Processing worldometer.info historical stats");
          covidTrackingUSStatesDataHistorical(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter);
        }
        log.info("Processing covidtracking.com data");
        // covidtracking.com (current)
        covidTrackingUSStatesData(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter);
        // WHO data
        log.info("Processing JHU WHO data");
        fetchAndProcessWHOData(httpClient, geoApiContext, wavefrontSender, ppsRateLimiter,
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/who_covid_19_situation_reports/who_covid_19_sit_rep_time_series/who_covid_19_sit_rep_time_series.csv",
            "confirmed");
      } catch (Exception ex) {
        log.log(Level.SEVERE, "Uncaught exception in run() loop", ex);
      }
      writeGeocodingResultsCacheFile();
      long elapsed = System.currentTimeMillis() - start;
      long toSleep = Math.max(0, TimeUnit.MINUTES.toMillis(5) - elapsed);
      log.info("Cycle Complete [" + elapsed + "ms], sleeping for: " + toSleep + "ms");
      Thread.sleep(toSleep);
    }
  }

  private void loadCachedGeocodingResults() throws IOException, ClassNotFoundException {
    if (Files.exists(Path.of(cachedGeocodingResultsFile))) {
      try (FileInputStream file = new FileInputStream(cachedGeocodingResultsFile)) {
        try (ObjectInputStream in = new ObjectInputStream(file)) {
          cachedGeocodingResults.putAll((Map<String, GeocodingResult[]>) in.readObject());
        }
      }
    }
    if (Files.exists(Path.of(cachedReverseGeocodingResultsFile))) {
      try (FileInputStream file = new FileInputStream(cachedReverseGeocodingResultsFile)) {
        try (ObjectInputStream in = new ObjectInputStream(file)) {
          cachedReverseGeocodingResults.putAll((Map<Pair<Double, Double>, AddressComponent[]>) in.readObject());
        }
      }
    }
  }

  private void writeGeocodingResultsCacheFile() throws IOException {
    try (FileOutputStream file = new FileOutputStream(cachedReverseGeocodingResultsFile)) {
      try (ObjectOutputStream out = new ObjectOutputStream(file)) {
        out.writeObject(cachedReverseGeocodingResults);
      }
    }
    try (FileOutputStream file = new FileOutputStream(cachedGeocodingResultsFile)) {
      try (ObjectOutputStream out = new ObjectOutputStream(file)) {
        out.writeObject(cachedGeocodingResults);
      }
    }
  }

  private void fetchFAHData(WavefrontSender wavefrontSender, RateLimiter ppsRateLimiter, OkHttpClient httpClient,
                            String team) {
    try (Response response = httpClient.newCall(
        new Request.Builder().url("https://api.foldingathome.org/team/" + team).get().build()).
        execute()) {
      if (response.code() != 200 || response.body() == null) {
        log.log(Level.WARNING, "Cannot fetch FAH data: " + team + " " + response.code());
      } else {
        String body = response.body().string();
        JsonNode jsonNode = OBJECT_MAPPER.readTree(body);
        String name = jsonNode.get("name").asText();
        long score = jsonNode.get("score").asLong();
        long wus = jsonNode.get("wus").asLong();
        long rank = jsonNode.get("rank").asLong();
        Map<String, String> tags = new HashMap<>();
        tags.put("version", "1");
        tags.put("name", name);
        tags.put("id", team);
        ppsRateLimiter.acquire(5);
        long time = System.currentTimeMillis();
        wavefrontSender.sendMetric("fah.team." + team + ".score", score, time, "api.foldingathome.org", tags);
        wavefrontSender.sendMetric("fah.team." + team + ".wus", wus, time, "api.foldingathome.org", tags);
        wavefrontSender.sendMetric("fah.team." + team + ".rank", rank, time, "api.foldingathome.org", tags);
      }
    } catch (Throwable e) {
      log.log(Level.SEVERE, "Uncaught exception when processing FAH data for team: " + team, e);
    }
    try (Response response = httpClient.newCall(
        new Request.Builder().url("https://api.foldingathome.org/team/" + team + "/members").get().build()).
        execute()) {
      if (response.code() != 200 || response.body() == null) {
        log.log(Level.WARNING, "Cannot fetch FAH data for team members: " + team + " " + response.code());
      } else {
        String body = response.body().string();
        JsonNode jsonNode = OBJECT_MAPPER.readTree(body);
        long time = System.currentTimeMillis();
        for (JsonNode child : jsonNode) {
          String name = child.get(0).asText();
          String id = child.get(1).asText();
          if (name.equals("name") && id.equals("id")) continue;
          long rank = child.get(2).asLong();
          long score = child.get(3).asLong();
          long wus = child.get(4).asLong();

          if (isBlank(name)) {
            name = "_blank_";
          }

          Map<String, String> tags = new HashMap<>();
          tags.put("version", "1");
          tags.put("name", name);
          tags.put("team", team);
          tags.put("id", id);
          ppsRateLimiter.acquire(3);
          wavefrontSender.sendMetric("fah.user." + name + ".score", score, time, "api.foldingathome.org", tags);
          wavefrontSender.sendMetric("fah.user." + name + ".wus", wus, time, "api.foldingathome.org", tags);
          wavefrontSender.sendMetric("fah.user." + name + ".rank", rank, time, "api.foldingathome.org", tags);
        }
      }
    } catch (Throwable e) {
      log.log(Level.SEVERE, "Uncaught exception when processing FAH data for team members: " + team, e);
    }
  }

  private void injectCountryStats(GeoApiContext geoApiContext,
                                  WavefrontSender wavefrontSender, RateLimiter ppsRateLimiter,
                                  boolean startFromBeginningOfYear) {
    try (InputStream inputStream = WavefrontCOVID19DataLoader.class.getResourceAsStream("/country data.csv")) {
      CSVParser records = CSVFormat.RFC4180.withFirstRecordAsHeader().withAllowMissingColumnNames().
          withTrim().parse(new InputStreamReader(inputStream));
      for (CSVRecord csvRecord : records) {
        String country = csvRecord.get(0);
        long population = Long.parseLong(csvRecord.get(1));
        long density = Long.parseLong(csvRecord.get(2));
        long landArea = Long.parseLong(csvRecord.get(3));
        int medianAge = Integer.parseInt(csvRecord.get(4));
        int urbanPercentage = Integer.parseInt(csvRecord.get(5));

        GeocodingResult[] geocodingResult = cachedGeocodingResults.computeIfAbsent("Country " + country, key -> {
          try {
            return GeocodingApi.geocode(geoApiContext, key).await();
          } catch (ApiException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
          }
        });
        if (geocodingResult.length > 0) {
          GeocodingResult firstResult = geocodingResult[0];
          Optional<AddressComponent> geocodedCountry = Arrays.stream(firstResult.addressComponents).
              filter(ac -> Arrays.stream(ac.types).anyMatch(act -> act == AddressComponentType.COUNTRY)).
              findFirst();
          if (geocodedCountry.isPresent()) {
            Map<String, String> tags = new HashMap<>();
            tags.put("version", "1");
            if (isNotBlank(geocodedCountry.get().longName)) {
              tags.put("country", geocodedCountry.get().longName);
            } else {
              tags.put("country", country);
            }
            if (isNotBlank(geocodedCountry.get().shortName)) {
              tags.put("country_short", geocodedCountry.get().shortName);
            }
            long timestamp = LocalDate.now(UTC).atTime(LocalDateTime.now(UTC).getHour(), 0).
                toEpochSecond(ZoneOffset.UTC);
            LocalDateTime date = LocalDateTime.of(2020, 1, 1, 0, 0);
            if (startFromBeginningOfYear) {
              while (true) {
                long oldTs = date.toEpochSecond(ZoneOffset.UTC);
                if (oldTs >= timestamp) break;
                date = date.plusHours(1);
                emitCountryStats(wavefrontSender, ppsRateLimiter, population, density, landArea, medianAge,
                    urbanPercentage, tags, oldTs);
              }
            }
            emitCountryStats(wavefrontSender, ppsRateLimiter, population, density, landArea, medianAge,
                urbanPercentage, tags, timestamp);
          } else {
            log.log(Level.WARNING, "Country: " + country + " cannot be geocoded");
          }
        } else {
          log.log(Level.WARNING, "Country: " + country + " cannot be geocoded");
        }
      }
    } catch (IOException e) {
      log.log(Level.SEVERE, "Uncaught exception when processing country data", e);
    }
  }

  private void emitCountryStats(WavefrontSender wavefrontSender, RateLimiter ppsRateLimiter, long population,
                                long density, long landArea, int medianAge, int urbanPercentage,
                                Map<String, String> tags, long timestamp) throws IOException {
    if (population > 0) {
      ppsRateLimiter.acquire();
      wavefrontSender.sendMetric("info.worldometers.population", population,
          timestamp, "worldometers.info", tags);
    }
    if (density > 0) {
      ppsRateLimiter.acquire();
      wavefrontSender.sendMetric("info.worldometers.density", density,
          timestamp, "worldometers.info", tags);
    }
    if (landArea > 0) {
      ppsRateLimiter.acquire();
      wavefrontSender.sendMetric("info.worldometers.land_area", landArea,
          timestamp, "worldometers.info", tags);
    }
    if (medianAge > 0) {
      ppsRateLimiter.acquire();
      wavefrontSender.sendMetric("info.worldometers.median_age", medianAge,
          timestamp, "worldometers.info", tags);
    }
    if (urbanPercentage > 0) {
      ppsRateLimiter.acquire();
      wavefrontSender.sendMetric("info.worldometers.urban_population_percentage", urbanPercentage,
          timestamp, "worldometers.info", tags);
    }
  }

  private void fetchAndProcessDailyCSSEData(OkHttpClient httpClient, GeoApiContext geoApiContext,
                                            WavefrontSender wavefrontSender, RateLimiter ppsRateLimiter,
                                            String url, String prefix, String csseDataVersion) {
    try (Response response = httpClient.newCall(
        new Request.Builder().url(url).get().build()).
        execute()) {
      if (response.code() != 200 || response.body() == null) {
        log.log(Level.WARNING, "Cannot fetch daily JHU data: " + url + " " + response.code());
      } else {
        String body = response.body().string();
        CSVParser records = CSVFormat.RFC4180.withFirstRecordAsHeader().withAllowMissingColumnNames().
            withTrim().parse(new StringReader(body));
        for (CSVRecord csvRecord : records) {
          String fips = csvRecord.get("FIPS");
          String admin2 = csvRecord.get("Admin2");
          String provinceState = csvRecord.get("Province_State");
          String countryRegion = csvRecord.get("Country_Region");
          double latitude = isNotBlank(csvRecord.get("Lat")) ? Double.parseDouble(csvRecord.get("Lat")) : 0;
          double longitude = isNotBlank(csvRecord.get("Long_")) ? Double.parseDouble(csvRecord.get("Long_")) : 0;
          Map<String, String> tags = new HashMap<>();
          // only add FIPS for US.
          if (isNotBlank(fips) && countryRegion.equals("US")) {
            tags.put("fips", fips);
          }
          long confirmed = Long.parseLong(csvRecord.get("Confirmed"));
          long deaths = Long.parseLong(csvRecord.get("Deaths"));
          long recovered = Long.parseLong(csvRecord.get("Recovered"));
          long active = Long.parseLong(csvRecord.get("Active"));
          LocalDateTime lastUpdate = parseCSSEDateTime(csvRecord.get("Last_Update"));
          ZonedDateTime utcDateTime = lastUpdate.atZone(UTC);

          if (countryRegion.equals("US")) {
            tags.put("csse_country", countryRegion);
            tags.put("country", "United States");
            tags.put("country_short", "US");
            // special cases for US in the JHU data.
            if (latitude == 0 && longitude == 0) {
              if (provinceState.equals("Recovered")) {
                tags.put("classifier", "Recovered");
                wavefrontSender.sendMetric(prefix + "csse." + csseDataVersion + ".recovered", recovered,
                    utcDateTime.toEpochSecond(), "csse", tags);
              } else {
                tags.put("csse_province", provinceState);
                if (isNotBlank(admin2)) {
                  tags.put("csse_admin2", admin2);
                  tags.put("admin2", admin2);
                }
                GeocodingResult[] geocodingResult = cachedGeocodingResults.computeIfAbsent(provinceState + ", US",
                    key -> {
                      try {
                        return GeocodingApi.geocode(geoApiContext, key).await();
                      } catch (ApiException | InterruptedException | IOException e) {
                        throw new RuntimeException(e);
                      }
                    });
                if (provinceState.equals("Wuhan Evacuee")) {
                  tags.put("classifier", "Wuhan Evacuee");
                } else if (provinceState.equals("Diamond Princess")) {
                  tags.put("classifier", "Diamond Princess");
                } else if (provinceState.equals("Grand Princess")) {
                  tags.put("classifier", "Grand Princess");
                } else {
                  Optional<AddressComponent> geocodedProvince = Arrays.stream(geocodingResult[0].addressComponents).
                      filter(ac -> Arrays.stream(ac.types).anyMatch(act ->
                          act == AddressComponentType.ADMINISTRATIVE_AREA_LEVEL_1)).
                      findFirst();
                  if (geocodedProvince.isPresent()) {
                    if (isBlank(geocodedProvince.get().longName)) {
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
                reportCSSEData(wavefrontSender, ppsRateLimiter, prefix, tags, confirmed, deaths, recovered, active,
                    utcDateTime, csseDataVersion);
              }
              continue;
            }
          }

          AddressComponent[] addressComponents =
              cachedReverseGeocodingResults.computeIfAbsent(Pair.of(latitude, longitude), latlong -> {
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
          // country
          if (!isBlank(countryRegion)) {
            tags.put("csse_country", countryRegion);
            Optional<AddressComponent> geocodedCountry = Arrays.stream(addressComponents).
                filter(ac -> Arrays.stream(ac.types).anyMatch(act -> act == AddressComponentType.COUNTRY)).
                findFirst();
            if (geocodedCountry.isPresent()) {
              if (isBlank(geocodedCountry.get().longName)) {
                tags.put("country", countryRegion);
              } else {
                tags.put("country", geocodedCountry.get().longName);
              }
              if (!isBlank(geocodedCountry.get().shortName)) {
                tags.put("country_short", geocodedCountry.get().shortName);
              }
            } else {
              tags.put("country", countryRegion);
            }
          }
          // state
          if (!isBlank(provinceState)) {
            tags.put("csse_province", provinceState);
            Optional<AddressComponent> geocodedProvince = Arrays.stream(addressComponents).
                filter(ac -> Arrays.stream(ac.types).anyMatch(act ->
                    act == AddressComponentType.ADMINISTRATIVE_AREA_LEVEL_1)).
                findFirst();
            if (geocodedProvince.isPresent()) {
              if (isBlank(geocodedProvince.get().longName)) {
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
          // county
          if (!isBlank(admin2)) {
            tags.put("csse_admin2", admin2);
            Optional<AddressComponent> geocodedAdmin2 = Arrays.stream(addressComponents).
                filter(ac -> Arrays.stream(ac.types).anyMatch(act ->
                    act == AddressComponentType.ADMINISTRATIVE_AREA_LEVEL_2)).
                findFirst();
            if (geocodedAdmin2.isPresent()) {
              if (isBlank(geocodedAdmin2.get().longName)) {
                tags.put("admin2", provinceState);
              } else {
                tags.put("admin2", geocodedAdmin2.get().longName);
              }
              if (isNotBlank(geocodedAdmin2.get().shortName) &&
                  !geocodedAdmin2.get().shortName.equals(tags.get("admin2"))) {
                tags.put("admin2_short", geocodedAdmin2.get().shortName);
              }
            } else {
              tags.put("admin2", provinceState);
            }
          }
          if (tags.get("country").equals("US")) {
            log.info(csvRecord.toString());
          } else {
            reportCSSEData(wavefrontSender, ppsRateLimiter, prefix, tags, confirmed, deaths, recovered, active,
                utcDateTime, csseDataVersion);
          }
        }
      }
    } catch (Exception ex) {
      log.log(Level.SEVERE, "Uncaught exception when processing CSSE data for: " + url, ex);
    }
  }

  @Nonnull
  private LocalDateTime parseCSSEDateTime(String dateTimeString) {
    try {
      return LocalDateTime.parse(dateTimeString, JHU_TS_V2);
    } catch (DateTimeParseException ex) {
      return LocalDateTime.parse(dateTimeString, JHU_TS_V2_1);
    }
  }

  private void reportCSSEData(WavefrontSender wavefrontSender, RateLimiter ppsRateLimiter, String prefix,
                              Map<String, String> tags, long confirmed, long deaths, long recovered, long active,
                              ZonedDateTime utcDateTime, String version) throws IOException {
    ppsRateLimiter.acquire(4);
    String host = "csse.";
    host = appendToHost(tags, host, "csse_country");
    host = appendToHost(tags, host, "csse_province");
    host = appendToHost(tags, host, "csse_admin2");
    wavefrontSender.sendMetric(prefix + "csse." + version + ".confirmed_cases", confirmed, utcDateTime.toEpochSecond(), host, tags);
    wavefrontSender.sendMetric(prefix + "csse." + version + ".deaths", deaths, utcDateTime.toEpochSecond(), host, tags);
    wavefrontSender.sendMetric(prefix + "csse." + version + ".recovered", recovered, utcDateTime.toEpochSecond(), host, tags);
    wavefrontSender.sendMetric(prefix + "csse." + version + ".active", active, utcDateTime.toEpochSecond(), host, tags);
  }

  private String appendToHost(Map<String, String> tags, String host, String key) {
    if (tags.containsKey(key) && isNotBlank(tags.get(key))) {
      host += tags.get(key) + ".";
    }
    return host;
  }

  private void fetchAndProcessCSSEData(OkHttpClient httpClient, GeoApiContext geoApiContext,
                                       WavefrontSender wavefrontSender, RateLimiter ppsRateLimiter,
                                       String url, String context, String prefix, String version) {
    try (Response response = httpClient.newCall(
        new Request.Builder().url(url).get().build()).
        execute()) {
      if (response.code() != 200 || response.body() == null) {
        log.log(Level.WARNING, "Cannot fetch data: " + url + " " + response.code());
      } else {
        String body = response.body().string();
        CSVParser records = CSVFormat.RFC4180.withFirstRecordAsHeader().withAllowMissingColumnNames().
            parse(new StringReader(body));
        List<String> headerNames = records.getHeaderNames();
        // Column 1 is Province/State, Column 2 is Country/Region, Column 3 is Lat and Column 4 is Long, then dates.
        Map<Integer, LocalDate> columnOrdinalToDates = new HashMap<>();
        for (int i = 4; i < headerNames.size(); i++) {
          String dateHeader = headerNames.get(i);
          if (!isBlank(dateHeader)) {
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
              cachedReverseGeocodingResults.computeIfAbsent(Pair.of(latitude, longitude), latlong -> {
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
          if (!isBlank(countryRegion)) {
            tags.put("csse_country", countryRegion);
            Optional<AddressComponent> geocodedCountry = Arrays.stream(addressComponents).
                filter(ac -> Arrays.stream(ac.types).anyMatch(act -> act == AddressComponentType.COUNTRY)).
                findFirst();
            if (geocodedCountry.isPresent()) {
              if (isBlank(geocodedCountry.get().longName)) {
                tags.put("country", countryRegion);
              } else {
                tags.put("country", geocodedCountry.get().longName);
              }
              if (!isBlank(geocodedCountry.get().shortName)) {
                tags.put("country_short", geocodedCountry.get().shortName);
              }
            } else {
              tags.put("country", countryRegion);
            }
          }
          if (!isBlank(provinceState)) {
            tags.put("csse_province", provinceState);
            Optional<AddressComponent> geocodedProvince = Arrays.stream(addressComponents).
                filter(ac -> Arrays.stream(ac.types).anyMatch(act ->
                    act == AddressComponentType.ADMINISTRATIVE_AREA_LEVEL_1)).
                findFirst();
            if (geocodedProvince.isPresent()) {
              if (isBlank(geocodedProvince.get().longName)) {
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
          for (int i = 4; i < csvRecord.size(); i++) {
            if (columnOrdinalToDates.containsKey(i) && !isBlank(csvRecord.get(i))) {
              int count = Integer.parseInt(csvRecord.get(i));
              LocalDate date = columnOrdinalToDates.get(i);
              // for march 23, 2020 and newer, we use the newer data format
              // https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/03-23-2020.csv
              if (date.isAfter(LocalDate.of(2020, 3, 22))) {
                break;
              }
              ZonedDateTime utcDateTime = date.atStartOfDay(UTC);
              ppsRateLimiter.acquire();
              String host = "csse.";
              host = appendToHost(tags, host, "csse_country");
              host = appendToHost(tags, host, "csse_province");
              host = appendToHost(tags, host, "csse_admin2");
              wavefrontSender.sendMetric(prefix + "csse." + version + "." + context, count, utcDateTime.toEpochSecond(), host, tags);
            }
          }
        }
      }
    } catch (Exception ex) {
      log.log(Level.SEVERE, "Uncaught exception when processing CSSE data: " + url, ex);
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
          if (!isBlank(dateHeader)) {
            LocalDate date;
            if (dateHeader.equals("19-Apr")) {
              date = LocalDate.of(2020, 4, 19);
            } else {
              date = LocalDate.parse(dateHeader, MONTH_DAY_YEAR);
            }
            columnOrdinalToDates.put(i, date);
          }
        }
        for (CSVRecord csvRecord : records) {
          String provinceState = csvRecord.get(0);
          String countryRegion = csvRecord.get(1);
          String whoRegion = csvRecord.get(2);
          String geocodingInput = isBlank(provinceState) ? "" : provinceState + ", ";
          geocodingInput += countryRegion;
          if (!isBlank(whoRegion)) {
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
          if (!isBlank(countryRegion)) {
            tags.put("who_country", countryRegion);
          }
          if (!isBlank(provinceState)) {
            tags.put("who_province_state", provinceState);
          }
          if (!isBlank(whoRegion)) {
            tags.put("who_region", whoRegion);
          }
          tags.put("version", "3");
          for (int i = 3; i < csvRecord.size(); i++) {
            if (columnOrdinalToDates.containsKey(i) && !isBlank(csvRecord.get(i))) {
              String value = csvRecord.get(i);
              // TODO handle cases like these better, for now, there's just one case in the WHO data.
              int count;
              if (value.equals("128(418)")) {
                count = 418;
              } else {
                count = Integer.parseInt(value);
              }
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
          Optional<Long> positive = jsonNodeToNumber(stateNode.get("positive"));
          Optional<Long> negative = jsonNodeToNumber(stateNode.get("negative"));
          Optional<Long> pending = jsonNodeToNumber(stateNode.get("pending"));
          Optional<Long> death = jsonNodeToNumber(stateNode.get("death"));
          Optional<Long> totalTestResults = jsonNodeToNumber(stateNode.get("totalTestResults"));
          Optional<Long> hospitalized = jsonNodeToNumber(stateNode.get("hospitalized"));
          Optional<Long> score = stateNode.has("score") ? jsonNodeToNumber(stateNode.get("score")) : empty();
          ZonedDateTime checkTimeEt = LocalDateTime.parse(stateNode.get("checkTimeEt").asText(), MONTH_DAY_HOUR_MINUTE).
              with(ChronoField.YEAR, 2020).atZone(ET);
          GeocodingResult[] geocodingResult = geocodeStateCode(geoApiContext, stateCode);
          Map<String, String> tags = new HashMap<>();
          tags.put("state_short", stateCode);
          if (geocodingResult.length > 0) {
            GeocodingResult firstResult = geocodingResult[0];
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
            if (positive.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.positive", positive.get(), timestamp, "covidtracking.com",
                  tags);
            }
            if (negative.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.negative", negative.get(), timestamp, "covidtracking.com",
                  tags);
            }
            if (pending.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.pending", pending.get(), timestamp, "covidtracking.com",
                  tags);
            }
            if (death.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.death", death.get(), timestamp, "covidtracking.com", tags);
            }
            if (totalTestResults.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.total", totalTestResults.get(), timestamp,
                  "covidtracking.com", tags);
            }
            if (hospitalized.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.hospitalized", hospitalized.get(), timestamp,
                  "covidtracking.com", tags);
            }
            if (score.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.score", score.get(), timestamp,
                  "covidtracking.com", tags);
            }
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
          Optional<Long> positive = jsonNodeToNumber(stateNode.get("positive"));
          Optional<Long> negative = jsonNodeToNumber(stateNode.get("negative"));
          Optional<Long> pending = jsonNodeToNumber(stateNode.get("pending"));
          Optional<Long> death = jsonNodeToNumber(stateNode.get("death"));
          Optional<Long> total = jsonNodeToNumber(stateNode.get("total"));
          Optional<Long> hospitalized = jsonNodeToNumber(stateNode.get("hospitalized"));
          LocalDateTime dateChecked = LocalDateTime.parse(stateNode.get("dateChecked").asText(),
              DateTimeFormatter.ISO_OFFSET_DATE_TIME);
          GeocodingResult[] geocodingResult = geocodeStateCode(geoApiContext, stateCode);
          Map<String, String> tags = new HashMap<>();
          tags.put("state_short", stateCode);
          if (geocodingResult.length > 0) {
            GeocodingResult firstResult = geocodingResult[0];
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
            if (positive.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.positive", positive.get(), timestamp, "covidtracking.com",
                  tags);
            }
            if (negative.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.negative", negative.get(), timestamp, "covidtracking.com",
                  tags);
            }
            if (pending.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.pending", pending.get(), timestamp, "covidtracking.com",
                  tags);
            }
            if (death.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.death", death.get(), timestamp, "covidtracking.com", tags);
            }
            if (total.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.total", total.get(), timestamp, "covidtracking.com", tags);
            }
            if (hospitalized.isPresent()) {
              wavefrontSender.sendMetric("com.covidtracking.hospitalized", hospitalized.get(), timestamp,
                  "covidtracking.com", tags);
            }
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

  private Optional<Long> jsonNodeToNumber(JsonNode node) {
    if (node == null || node.isNull()) return empty();
    return Optional.of(Long.parseLong(node.asText()));
  }
}
