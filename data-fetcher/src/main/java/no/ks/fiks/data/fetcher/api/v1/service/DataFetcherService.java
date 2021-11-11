package no.ks.fiks.data.fetcher.api.v1.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ks.fiks.Service.InsertTableService;
import no.ks.fiks.database.service.api.v1.config.SqlConfiguration;
import no.ks.fiks.ssbAPI.APIService.SsbApiCall;
import no.ks.fiks.ssbAPI.metadataApi.SsbMetadata;
import no.ks.fiks.ssbAPI.metadataApi.SsbMetadataVariables;

import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.Buffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class DataFetcherService {
    private final SqlConfiguration config;

    public DataFetcherService() {
        config = new SqlConfiguration();
    }

    public String createTable(String jsonPayload) {
        String tableName, query, columnDeclarations, result, tableCode;
        Map<String, List<String>> filters;
        SsbApiCall sac;

        tableCode = getTableCode(jsonPayload);

        System.out.println("Create table: " + tableCode);

        if (tableCode == null)
            return "[ERROR] The json doesn't have the tableCode field.";

        tableName = String.format("%s.SSB_%s", config.getSchemaName(), tableCode);

        filters = getFilters(jsonPayload);

        sac = fetchSsbApiCallData(tableCode,1, filters);

        if (sac == null)
            return "[ERROR] Something went wrong while fetching the SsbApiCall data.";

        columnDeclarations = createColumnDeclarations(sac.getMetadata());

        //create the table in the db
        query = String.format("create table %s (%s, [Verdi] [numeric] (18,1))", tableName, columnDeclarations);

        result = apiCall("create-table", query);

        return result;
    }

    public String insertData(String jsonPayload) {
        String tableCode, tableName;
        int numberOfYears;
        Map<String, List<String>> filters;
        List<Map<String[], BigDecimal>> dataResult;
        SsbApiCall sac;

        tableCode = getTableCode(jsonPayload);

        if (tableCode == null)
            return "[ERROR] The json doesn't have the tableCode field.";

        tableName = String.format("%s.SSB_%s", config.getSchemaName(), tableCode);
        numberOfYears = getNumberOfYears(jsonPayload);

        filters = getFilters(jsonPayload);

        sac = fetchSsbApiCallData(tableCode, numberOfYears, filters);

        if (sac == null)
            return "[ERROR] Failed while fetching SsbApiCall data.";

        dataResult = fetchAndStructureSsbApiCallResult(sac);

        if (dataResult == null)
            return "[ERROR] Failed while fetching and structuring data.";

        String result ="";
        for(String s: fetchSsbApiCallResult(sac)) {
            result = apiCall("insert-data", createInsertJson(tableName, s));
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
        String dropQuery = String.format("drop table %s.[SSB_%s]", config.getSchemaName(), getTableCode(jsonPayload));
        String result = apiCall("drop-table", dropQuery);
        return result;
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
        String truncateQuery = String.format("truncate table %s.[SSB_%s]", config.getSchemaName(), getTableCode(jsonPayload));
        String result = apiCall("truncate-table", truncateQuery);
        return result;
    }


    /**
     * Gets the value of the tableCode field in the json string provided.
     * If tableCode aren't specified then returns null.
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
     * Gets the value of the numberOfYears field in the json string provided.
     * If numberOfYears aren't specified then returns 5.
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

            for (JsonNode filterObject: actualObj.get("filters")) {
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
     *
     * For each metadata code in the metadata fetched creates a pair of columns on the form:
     * [[metadata name]navn] [varchar] (largestValue)
     * [[metadata name]kode] [varchar] (largestValue)
     * e.g
     * [Regionkode] [varchar] (5)
     * [Regionnavn] [varchar] (24)
     *
     * If the metadata column is "Tid" then it uses the metadata code instead.
     *
     * @param metadata the object containing all the metadata information
     * @return the column declarations on the mentioned form
     */
    private String createColumnDeclarations(SsbMetadata metadata) {
        StringBuilder columnDeclarations = new StringBuilder();

        Iterator<SsbMetadataVariables> iterator = metadata.getVariables().iterator();

        while (iterator.hasNext()) {
            SsbMetadataVariables smv = iterator.next();

            if (smv.getCode().equals("Tid")) {
                columnDeclarations.append(String.format("[%skode] [varchar] (%s), ", StringUtils.capitalize(smv.getCode()),
                        smv.getLargestValue()));
                columnDeclarations.append(String.format("[%snavn] [varchar] (%s)", StringUtils.capitalize(smv.getCode()),
                        smv.getLargestValueText()));
            } else {
                columnDeclarations.append(String.format("[%skode] [varchar] (%s), ", StringUtils.capitalize(smv.getText()),
                        smv.getLargestValue()));
                columnDeclarations.append(String.format("[%snavn] [varchar] (%s)", StringUtils.capitalize(smv.getText()),
                        smv.getLargestValueText()));
            }

            if (iterator.hasNext()) {
                columnDeclarations.append(", ");
            }
        }
        return columnDeclarations.toString();
    }

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
            return "fail";
        }
    }

    private List<Map<String[], BigDecimal>> fetchAndStructureSsbApiCallResult(SsbApiCall sac) {
        InsertTableService its = new InsertTableService();
        try {
            System.out.println("Fetching data");
            List<String> jsonStatQuery = sac.tableApiCall();

            System.out.println("Structuring the json data");
            return its.structureJsonStatTable(jsonStatQuery);
        } catch (IOException ie) {
            System.out.println("IOException in fetchAndStructureSsbApiCallResult.");
            return null;
        }
    }

    private List<String> fetchSsbApiCallResult(SsbApiCall sac) {
        try {
            System.out.println("Fetching data");
            return sac.tableApiCall();
        } catch (IOException ie) {
            System.out.println("IOException in fetchSsbApiCallResult.");
            return null;
        }
    }

    private SsbApiCall fetchSsbApiCallData(String tableCode, int numberOfYears, Map<String, List<String>> filters) {
        SsbApiCall sac;

        try {
            sac = new SsbApiCall(tableCode, numberOfYears,
                    "131", "104", "214", "231", "127");

            if (filters != null)
                sac.metadataApiCall(filters, true);
            else
                sac.metadataApiCall(tableCode);

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

    private String apiCall(String endpoint, String payload) {
        URL url = null;
        try {
            url = new URL("http://localhost:8080/api/v1/" + endpoint);

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json; utf-8");
            connection.setRequestProperty("Accept", "application/json");
            connection.setDoOutput(true);

            OutputStream os = connection.getOutputStream();
            byte[] input = payload.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);

            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(),
                    StandardCharsets.UTF_8));

            String var9;

            StringBuilder response = new StringBuilder();

            while(true) {
                String responseLine;
                if ((responseLine = br.readLine()) == null) {
                    var9 = response.toString();
                    break;
                }

                response.append(responseLine.trim());
            }
            connection.disconnect();

            return var9;
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;

    }
}
