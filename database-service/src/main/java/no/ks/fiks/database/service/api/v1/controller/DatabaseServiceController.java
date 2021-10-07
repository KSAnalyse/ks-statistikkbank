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
    private SsbApiCall sac;
    private InsertTableService its;

    private final JdbcTemplate jdbcTemplate;

    private DatabaseService dbs = new DatabaseService(new SqlConfiguration());

    public DatabaseServiceController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * @return
     */
    @GetMapping("/test")
    public String test() {
        return "yep";
    }

    /**
     * @param createString
     * @return
     */
    @PostMapping("/create-table")
    public String createTable(@Valid @RequestBody String createString) {
        String tableName, query, columnDeclarations;
        Map<String, List<String>> filters;
        String result;

        try {
            tableName = "ssbks.SSB_" + getTableCode(createString);

            sac = new SsbApiCall(getTableCode(createString), getNumberOfYears(createString),
                    "131","104", "214", "231", "127");
            filters = getFilters(createString);

            if (filters != null)
                sac.metadataApiCall(filters, false);
            else
                sac.metadataApiCall(getTableCode(createString));

            columnDeclarations = createColumnDeclarations(sac.getMetadata());

            //drop the table in case it already exists
            query = String.format("drop table %s", tableName);
            result = dbs.checkQuery(jdbcTemplate, query);

            if (!result.equals("OK"))
                return result;

            //create the table in the db
            query = String.format("create table %s (%s, [Verdi] [numeric] (18,1))", tableName, columnDeclarations);
            result = dbs.checkQuery(jdbcTemplate, query);

            if (!result.equals("OK"))
                return result;

            its = new InsertTableService();

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
     * @param dropTable
     * @return
     */
    @PostMapping("/drop-table")
    public String dropTable(@Valid @RequestBody String dropTable) {
        return dbs.checkQuery(jdbcTemplate, dropTable);
    }

    /**
     * @param truncateTable
     * @return
     */
    @PostMapping("/truncate-table")
    public String truncateTable(@Valid @RequestBody String truncateTable) {
        return dbs.checkQuery(jdbcTemplate, truncateTable);
    }

    /**
     * Gets the value of the tableCode field in the json string provided.
     *
     * @param createString the json string
     * @return the value of tableCode as text
     * @throws JsonProcessingException
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
     * @throws JsonProcessingException
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
     * @throws JsonProcessingException
     */
    private Map<String, List<String>> getFilters(String createString) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(createString);

        Map<String, List<String>> filterMap = new HashMap<String, List<String>>();

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
     * [[metadata name]navn] [varchar] (largestvalue)
     * [[metadata name]kode] [varchar] (largestvalue)
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
        String columnDeclarations ="";

        Iterator<SsbMetadataVariables> iterator = metadata.getVariables().iterator();

        while (iterator.hasNext()) {
            SsbMetadataVariables smv = iterator.next();
            switch (smv.getCode()) {
                case "Tid" -> {
                    columnDeclarations += String.format("[%skode] [varchar] (%s), ", StringUtils.capitalize(smv.getCode()),
                            smv.getLargestValue());
                    columnDeclarations += String.format("[%snavn] [varchar] (%s)", StringUtils.capitalize(smv.getCode()),
                            smv.getLargestValueText());
                    break;
                }
                default -> {
                    columnDeclarations += String.format("[%skode] [varchar] (%s), ", StringUtils.capitalize(smv.getText()),
                            smv.getLargestValue());
                    columnDeclarations += String.format("[%snavn] [varchar] (%s)", StringUtils.capitalize(smv.getText()),
                            smv.getLargestValueText());
                    break;
                }
            }

            if (iterator.hasNext()) {
                columnDeclarations += ", ";
            }
        }
        return columnDeclarations;
    }
}
