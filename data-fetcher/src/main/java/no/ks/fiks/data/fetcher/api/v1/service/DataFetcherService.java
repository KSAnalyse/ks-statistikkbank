package no.ks.fiks.data.fetcher.api.v1.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ks.fiks.Service.InsertTableService;
import no.ks.fiks.data.fetcher.api.v1.scheduler.Scheduler;
import no.ks.fiks.data.fetcher.api.v1.scheduler.ThreadQuery;
import no.ks.fiks.data.fetcher.csvreader.CsvReader;
import no.ks.fiks.data.fetcher.tableinfo.TabellFilter;
import no.ks.fiks.ssbAPI.APIService.SsbApiCall;
import no.ks.fiks.ssbAPI.metadataApi.SsbMetadata;
import no.ks.fiks.ssbAPI.metadataApi.SsbMetadataVariables;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.*;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class DataFetcherService {

    private final Scheduler scheduler;
    private final String username;
    private final String password;
    private String apiToken;
    private LocalDateTime lastTokenFetch;
    private final List<TabellFilter> tabellFilter;

    public DataFetcherService() {
        //TODO: Get the fields from a config file
        CsvReader csvReader = new CsvReader();
        Properties properties = new Properties();
        try {
            csvReader.readFromCsv();
            properties.load(new FileInputStream("src/test/resources/login.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        scheduler = new Scheduler();
        tabellFilter = csvReader.getTablesAndFilters();
        username = properties.getProperty("username");
        password = properties.getProperty("password");
    }

    /**
     * Creates the table specified in the json payload.
     * Fetches metadata for the table from SSB by using the tableCode field and eventual filters specified in the
     * json payload. Creates a create query with the column metadata fetched from SSB before doing a post call to the
     * database-service API.
     *
     * @param jsonPayload the json containing all the information needed.
     * @return the result from the database-service API
     * @see #fetchSsbApiCallData(String, int, Map)
     * @see #createColumnDeclarations(SsbMetadata, Map)
     * @see #apiCall(String, String)
     */
    public String createTable(String jsonPayload) {
        String tableName, query, columnDeclarations, tableCode, schemaName;
        SsbApiCall sac;

        tableCode = getTableCode(jsonPayload);
        if (tableCode == null)
            return "[ERROR] The json doesn't have the tableCode field.";

        schemaName = getSchemaName(jsonPayload);
        if (schemaName == null)
            return "[ERROR] The json doesn't have the schemaName field.";

        //sac = fetchSsbApiCallData(tableCode, 1, getFilters(jsonPayload));

        tableName = String.format("%s.[%s]", schemaName, tableCode);
        scheduler.addThreadToQueue(new ThreadQuery(tableCode, tableName, "create", getFilters(jsonPayload)));

        /*
        sac = fetchSsbApiCallData(tableCode, 1, getFilters(jsonPayload));
        if (sac == null)
            return "[ERROR] Something went wrong while fetching the SsbApiCall data.";
        Map<String, List<String>> filter = new LinkedHashMap<>();
        tableName = String.format("%s.[%s]", schemaName, tableCode);
        columnDeclarations = createColumnDeclarations(sac.getMetadata(), filter);
        query = String.format("create table %s (%s, [Verdi] [numeric] (18,1))", tableName, columnDeclarations);

        return apiCall("create-table", query);
        */
        return "TODO";
    }

    /**
     * Inserts data fetched from SSB into a table in the database.
     * Creates a query to SSB using the information found in the json before calling the database-service API.
     *
     * @param jsonPayload the json containing all the information needed to fetch data from SSB
     * @return the result from the database-service API
     */
    public String insertData(String jsonPayload) {
        String tableCode, tableName, schemaName;
        SsbApiCall sac;

        tableCode = getTableCode(jsonPayload);
        if (tableCode == null)
            return "[ERROR] The json doesn't have the tableCode field.";

        schemaName = getSchemaName(jsonPayload);
        if (schemaName == null)
            return "[ERROR] The json doesn't have the schemaName field.";

        TabellFilter tableObject = tabellFilter.stream().filter(table -> table.getTabellnummer().equals(tableCode)).findAny().orElse(null);

        sac = fetchSsbApiCallData(tableCode, getNumberOfYears(jsonPayload), tableObject.getHentDataFilter());
        if (sac == null)
            return "[ERROR] Failed while fetching SsbApiCall data.";

        String result = "";
        tableName = String.format("%s.[%s]", schemaName, tableCode);
        for (String s : fetchSsbApiCallResult(sac)) {
            result = apiCall("insert-data", createInsertJson(tableName, s));

            if (!result.equals("OK"))
                return result;
        }
        return result;
    }

    /**
     * Drops the table specified in the json payload.
     * Creates a drop query using the tableCode field in the payload before doing a post call to the database-service
     * API.
     *
     * @param jsonPayload
     * @return a string with the result
     * @see #apiCall(String, String)
     */
    public String dropTable(String jsonPayload) {
        String schemaName, tableCode;

        tableCode = getTableCode(jsonPayload);
        if (tableCode == null)
            return "[ERROR] The json doesn't have the tableCode field.";

        schemaName = getSchemaName(jsonPayload);
        if (schemaName == null)
            return "[ERROR] The json doesn't have the schemaName field.";

        String dropQuery = String.format("drop table %s.[%s]", getSchemaName(jsonPayload), getTableCode(jsonPayload));
        return apiCall("drop-table", dropQuery);
    }

    /**
     * Truncates the table specified in the json payload.
     * Creates a truncate query using the tableCode field in the payload before doing a post call to the
     * database-service API.
     *
     * @param jsonPayload the JSON containing the tableCode field
     * @return a string with the result
     * @see #apiCall(String, String)
     */
    public String truncateTable(String jsonPayload) {
        String schemaName, tableCode;

        tableCode = getTableCode(jsonPayload);
        if (tableCode == null)
            return "[ERROR] The json doesn't have the tableCode field.";

        schemaName = getSchemaName(jsonPayload);
        if (schemaName == null)
            return "[ERROR] The json doesn't have the schemaName field.";

        String truncateQuery = String.format("truncate table %s.[%s]", schemaName, tableCode);
        return apiCall("truncate-table", truncateQuery);
    }

    /**
     * Gets the value of the tableCode field in the json string provided.
     * If tableCode aren't specified then returns null.
     *
     * @param createString the json string
     * @return the value of tableCode as text or null if it doesn't exist
     */
    private String getTableCode(String createString) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode actualObj = mapper.readTree(createString);

            if (actualObj.get("tableCode") == null)
                return null;

            return actualObj.get("tableCode").asText();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return ("[ERROR] JsonProcessingException when fetching table code from json object.");
        } catch (NullPointerException ne) {
            ne.printStackTrace();
            return ("[ERROR] NullPointerException when fetching table code from json object.");
        }
    }

    /**
     * Gets the value of the tableCode field in the json string provided.
     * If tableCode aren't specified then returns null.
     *
     * @param createString the json string
     * @return the value of tableCode as text or null if it doesn't exist
     */
    private String getSchemaName(String createString) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode actualObj = mapper.readTree(createString);

            if (actualObj.get("schemaName") == null)
                return null;

            return actualObj.get("schemaName").asText();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return ("[ERROR] JsonProcessingException when fetching schema name from json object.");
        } catch (NullPointerException ne) {
            ne.printStackTrace();
            return ("[ERROR] NullPointerException when fetching schema name from json object.");
        }
    }

    /**
     * Gets the value of the numberOfYears field in the json string provided.
     * If numberOfYears aren't specified then returns 5.
     *
     * @param createString the json string
     * @return the numberOfYears value as int or 5 if it is non-existing
     */
    private int getNumberOfYears(String createString) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode actualObj = mapper.readTree(createString);

            if (actualObj.get("numberOfYears") == null)
                return 5;

            return actualObj.get("numberOfYears").asInt();
        } catch (Exception e) {
            e.printStackTrace();
            return 5;
        }
    }

    /**
     * Gets the value of the filters field in the json string provided and returns it as a map.
     * If the filters field doesn't have any values returns null.
     *
     * @param createString the json string
     * @return the map containing the values of the filters field or null if no filters are specified
     */
    private Map<String, List<String>> getFilters(String createString) {
        ObjectMapper mapper = new ObjectMapper();

        try {
            JsonNode actualObj = mapper.readTree(createString);

            Map<String, List<String>> filterMap = new HashMap<>();

            if (actualObj.get("filters") == null)
                return null;

            for (JsonNode filterObject : actualObj.get("filters")) {
                List<String> values = new LinkedList<>();
                for (JsonNode filterValue : filterObject.get("values")) {
                    values.add(filterValue.asText());
                }
                filterMap.put(filterObject.get("code").asText(), values);
            }
            return filterMap;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Creates the column declarations needed for the create query based on the metadata provided.
     * <p>
     * For each metadata code in the metadata fetched creates a pair of columns on the form:
     * [[metadata name]navn] [varchar] (largestValue)
     * [[metadata name]kode] [varchar] (largestValue)
     * e.g
     * [Regionkode] [varchar] (5)
     * [Regionnavn] [varchar] (24)
     * <p>
     * If the metadata column is "Tid" then it uses the metadata code instead.
     *
     * @param metadata the object containing all the metadata information
     * @return the column declarations on the mentioned form
     */
    public String createColumnDeclarations(SsbMetadata metadata, Map<String, List<String>> lagDataFilter) {
        StringBuilder columnDeclarations = new StringBuilder();
        List<String> skipDimension = new LinkedList<>();
        Iterator<SsbMetadataVariables> iterator = metadata.getVariables().iterator();
        if (!lagDataFilter.isEmpty()) {
            for (String key : lagDataFilter.keySet()) {
                if (key.equalsIgnoreCase("add")) {
                    columnDeclarations.append(String.format("[%skode] [varchar] (%s), ", StringUtils.capitalize(lagDataFilter.get(key).get(0)),
                            lagDataFilter.get(key).get(1)));
                    columnDeclarations.append(String.format("[%snavn] [varchar] (%s), ", StringUtils.capitalize(lagDataFilter.get(key).get(0)),
                            lagDataFilter.get(key).get(1)));
                } else if (!lagDataFilter.get(key).isEmpty() && lagDataFilter.get(key).get(0).equalsIgnoreCase("none")) {
                    skipDimension.add(key);
                    System.out.println(skipDimension);
                }
            }
        }
        while (iterator.hasNext()) {
            SsbMetadataVariables smv = iterator.next();

            if (smv.getCode().equals("Tid")) {
                columnDeclarations.append(String.format("[%skode] [varchar] (%s), ", StringUtils.capitalize(smv.getCode()),
                        smv.getLargestValue()));
                columnDeclarations.append(String.format("[%snavn] [varchar] (%s)", StringUtils.capitalize(smv.getCode()),
                        smv.getLargestValueText()));
            } else {
                if (!skipDimension.contains(smv.getCode())) {
                    columnDeclarations.append(String.format("[%skode] [varchar] (%s), ", StringUtils.capitalize(smv.getText()),
                            smv.getLargestValue()));
                    columnDeclarations.append(String.format("[%snavn] [varchar] (%s)", StringUtils.capitalize(smv.getText()),
                            smv.getLargestValueText()));
                }
            }

            if (iterator.hasNext()) {
                columnDeclarations.append(", ");
            }
        }
        return columnDeclarations.toString();
    }

    /**
     * Creates a json string with the fields tableName and data.
     *
     * @param tableName   the name of the table in the db
     * @param queryResult the json-stat2 containing the data
     * @return returns the new json string if successful, else an error message
     */
    private String createInsertJson(String tableName, String queryResult) {
        ObjectMapper om = new ObjectMapper();

        try {
            ObjectNode on = om.createObjectNode();
            JsonNode resultData = om.readTree(queryResult);

            on.put("tableName", tableName);
            on.set("data", resultData);

            return om.writeValueAsString(on);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "[ERROR] Issues occurred while processing the json.";
        }
    }

    /**
     * Fetches and structures data from SSB and returns the result.
     * Uses the ssbAPI and Service package to fetch and structure the data.
     *
     * @param sac the SsbApiCall object containing all the information needed for the fetch.
     * @return a list containing a map with values per row
     */
    private List<Map<String[], BigDecimal>> fetchAndStructureSsbApiCallResult(SsbApiCall sac) {
        InsertTableService its = new InsertTableService();
        try {
            List<String> jsonStatQuery = sac.tableApiCall();

            return its.structureJsonStatTable(jsonStatQuery);
        } catch (IOException ie) {
            System.out.println("IOException in fetchAndStructureSsbApiCallResult.");
            return null;
        }
    }

    /**
     * Fetches data from a specific table from SSB using the SsbApiCall class.
     *
     * @param sac the SsbApiCall object containing the information about the table to fetch data from
     * @return the result of the fetch, null if something went wrong
     */
    private List<String> fetchSsbApiCallResult(SsbApiCall sac) {
        try {
            return sac.tableApiCall();
        } catch (IOException ie) {
            System.out.println("IOException in fetchSsbApiCallResult.");
            return null;
        }
    }

    /**
     * Fetches metadata from a specific table from SSB using the SsbApiCall class.
     * If there's specified any filters applies these with the removeAllBut parameter set to true.
     *
     * @param tableCode     the table code to the table at SSB
     * @param numberOfYears the number of years to fetch data from
     * @param filters       filters to be applied when fetching data
     * @return the object containing all the information fetched from SSB
     */
    private SsbApiCall fetchSsbApiCallData(String tableCode, int numberOfYears, Map<String, List<String>> filters) {
        SsbApiCall sac;

        try {
            sac = new SsbApiCall(tableCode, numberOfYears,
                    "131", "104", "214", "231", "127");

            if (filters != null)
                sac.metadataApiCall(filters, true);

            return sac;
        } catch (IOException ie) {
            Matcher matcher = Pattern.compile("^Server returned HTTP response code: (\\d+)").matcher(ie.getMessage());

            if (matcher.find()) {
                System.out.println("[ERROR] Status code from exception: " + Integer.parseInt(matcher.group(1)));
            } else if (ie.getClass().getSimpleName().equals("FileNotFoundException")) {
                // 404 will throw a FileNotFoundException
                System.out.println("[ERROR] Status code from exception: 404");
            } else {
                System.out.println("[ERROR] " + ie.getClass().getSimpleName() + " exception when creating table.");
            }

            return null;
        }
    }

    /**
     * Connects and posts to the database-service API.
     * Connects to the endpoint given and posts the payload.
     *
     * @param endpoint the endpoint to where to do the post request
     * @param payload  the payload to be posted
     * @return the response from the API
     */
    private String apiCall(String endpoint, String payload) {


        long expirationSeconds = 10800;
        if (lastTokenFetch == null || lastTokenFetch.isBefore(LocalDateTime.now().minus(expirationSeconds, ChronoUnit.SECONDS)))
            fetchToken();


        /*if (Calendar.getInstance().getTime().getTime() - lastTokenFetch.getTime() >= 10600000 || lastTokenFetch == null) {
            fetchToken();
        }*/

        try {
            URL url = new URL("http://localhost:8080/api/v1/" + endpoint);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json; utf-8");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Authorization", apiToken);
            connection.setDoOutput(true);

            OutputStream os = connection.getOutputStream();
            byte[] input = payload.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);

            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(),
                    StandardCharsets.UTF_8));

            String responseString;
            StringBuilder response = new StringBuilder();

            while (true) {
                String responseLine;
                if ((responseLine = br.readLine()) == null) {
                    responseString = response.toString();
                    break;
                }

                response.append(responseLine.trim());
            }
            connection.disconnect();

            return responseString;
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return "[ERROR]: MalformedURLException in apiCall.";
        } catch (IOException e) {
            e.printStackTrace();
            return addRedColorToString("[ERROR]: IOException in apiCall.");
        }
    }

    private void fetchToken() {
        System.out.println("[fetchToken] Fetching token!");

        try {

            lastTokenFetch = LocalDateTime.now();
            URL url;
            url = new URL("http://localhost:8080/public/users/login"/*?email=" + username + "&password=" + password*/);
            String encoding = Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json; utf-8");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Authorization", "Basic" + encoding);
            connection.setDoOutput(true);

            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(),
                    StandardCharsets.UTF_8));

            String responseString;
            StringBuilder response = new StringBuilder();

            while (true) {
                String responseLine;
                if ((responseLine = br.readLine()) == null) {
                    responseString = response.toString();
                    break;
                }

                response.append(responseLine.trim());
            }
            connection.disconnect();

            System.out.println("Check out this responseString!: " + responseString);
            apiToken = responseString;
            //return responseString;
        } catch (MalformedURLException e) {
            e.printStackTrace();
            System.out.println("[ERROR]: MalformedURLException in apiCall.");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(addRedColorToString("[ERROR]: IOException in apiCall."));
        }
    }

    private String addRedColorToString(String message) {
        return "\u001B[31m" + message + "\u001B[0m";
    }
}
