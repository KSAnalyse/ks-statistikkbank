package no.ks.fiks.database.service.api.v1.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ks.fiks.database.service.api.v1.config.SqlConfiguration;
import no.ks.fiks.database.service.api.v1.service.DatabaseService;
import no.ks.fiks.Service.InsertTableService;
import no.ks.fiks.ssbAPI.APIService.SsbApiCall;

import no.ks.fiks.ssbAPI.metadataApi.SsbMetadata;
import no.ks.fiks.ssbAPI.metadataApi.SsbMetadataVariables;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

@RestController
@RequestMapping("/api/v1")
public class DatabaseServiceController {
    private final JdbcTemplate jdbcTemplate;

    private final DatabaseService dbs = new DatabaseService(new SqlConfiguration());

    public DatabaseServiceController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * @param createString the table code of the table to be created
     * @return result of running the create query
     */
    @PostMapping("/create-table")
    public String createTable(@Valid @RequestBody String createString) {
        SsbApiCall sac;
        String tableName, query, columnDeclarations, result;
        Map<String, List<String>> filters;

        try {
            System.out.println("Create table: " + getTableCode(createString));
            tableName = "ssbks.SSB_" + getTableCode(createString);

             sac = new SsbApiCall(getTableCode(createString), getNumberOfYears(createString),
                    "131", "104", "214", "231", "127");
            filters = getFilters(createString);

            if (filters != null)
                sac.metadataApiCall(filters, false);
            else
                sac.metadataApiCall(getTableCode(createString));

            columnDeclarations = createColumnDeclarations(sac.getMetadata());

            //drop the table in case it already exists
            query = String.format("drop table %s", tableName);
            dbs.checkQuery(jdbcTemplate, query);

            //create the table in the db
            query = String.format("create table %s (%s, [Verdi] [numeric] (18,1))", tableName, columnDeclarations);
            result = dbs.checkQuery(jdbcTemplate, query);

            if (!result.equals("OK"))
                return result;

            InsertTableService its = new InsertTableService();

            System.out.println("Fetching data");
            List<String> jsonStatQuery = sac.tableApiCall();

            System.out.println("Structuring the json data");
            List<Map<String[], BigDecimal>> ssbResult = its.structureJsonStatTable(jsonStatQuery);

            return dbs.checkQuery(jdbcTemplate, ssbResult, tableName);

        } catch (IOException ie) {
            return("IOException in createTable.");
        }

    }

    /**
     * @param dropTable the table code of the table to be dropped
     * @return result of running the truncate query
     * @see DatabaseService
     */
    @PostMapping("/drop-table")
    public String dropTable(@Valid @RequestBody String dropTable) {
        String dropQuery = String.format("drop table ssbks.[SSB_%s]", dropTable);
        return dbs.checkQuery(jdbcTemplate, dropQuery);
    }

    /**
     * @param truncateTable the table code of the table to be truncated
     * @return result of running the truncate query
     * @see DatabaseService
     */
    @PostMapping("/truncate-table")
    public String truncateTable(@Valid @RequestBody String truncateTable) {
        String truncateQuery = String.format("truncate table ssbks.[SSB_%s]", truncateTable);
        return dbs.checkQuery(jdbcTemplate, truncateQuery);
    }

    /**
     * Gets the value of the tableCode field in the json string provided.
     *
     * @param createString the json string
     * @return the value of tableCode as text
     * @throws JsonProcessingException if something went wrong with processing the json
     */
    private String getTableCode(String createString) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(createString);

        if (actualObj.get("tableCode") == null)
            return null;

        return actualObj.get("tableCode").asText();

    }

    /**
     * Gets the value of the numberOfYears field in the json string provided.
     *
     * @param createString the json string
     * @return the numberOfYears value as int
     * @throws JsonProcessingException if something went wrong with processing the json
     */
    private int getNumberOfYears(String createString) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(createString);

        if (actualObj.get("numberOfYears") == null)
            return 5;

        return actualObj.get("numberOfYears").asInt();
    }

    /**
     * Gets the value of the filters field in the json string provided and returns it as a map.
     *
     * @param createString the json string
     * @return the map containing the values of the filters field
     * @throws JsonProcessingException if something went wrong with processing the json
     */
    private Map<String, List<String>> getFilters(String createString) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
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
}
