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
import org.springframework.util.StopWatch;
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

    @GetMapping("/test")
    public String test() {
        return "yep";
    }

    @PostMapping("/create-table")
    public String createTable(@Valid @RequestBody String createString) {
        String tableName, query, columnDeclarations;
        Map<String, List<String>> filters;
        StopWatch timer = new StopWatch();
        String result;

        try {
            tableName = "ssbks.SSB_" + getTableCode(createString);
            System.out.println(tableName);
            sac = new SsbApiCall(getTableCode(createString), getNumberOfYears(createString),
                    "131","104", "214", "231", "127");
            filters = getFilters(createString);

            if (filters != null)
                sac.metadataApiCall(filters, false);
            else
                sac.metadataApiCall(getTableCode(createString));

            columnDeclarations = createColumnDeclarations(sac.getMetadata());

            query = String.format("drop table %s", tableName);
            result = dbs.checkQuery(jdbcTemplate, query);

            if (!result.equals("OK"))
                return result;

            query = String.format("create table %s (%s, [Verdi] [numeric] (18,1))", tableName, columnDeclarations);
            result = dbs.checkQuery(jdbcTemplate, query);

            if (!result.equals("OK"))
                return result;

            its = new InsertTableService();

            System.out.println("Fetching actual data");
            timer.start();
            List<String> jsonStatQuery = sac.tableApiCall();
            timer.stop();
            System.out.println("Actual data fetched: " + timer.getTotalTimeSeconds());

            System.out.println("Structuring the json data");
            timer = new StopWatch();
            timer.start();
            List<Map<String[], BigDecimal>> ssbResult = its.structureJsonStatTable(jsonStatQuery);
            timer.stop();
            System.out.println("Structured data: " + timer.getTotalTimeSeconds());

            System.out.println("result size:" + ssbResult.size());

            return dbs.checkQuery(jdbcTemplate, ssbResult, tableName);

        } catch (IOException ie) {
            return("IOException in createTable.");
        }

    }

    @PostMapping("/drop-table")
    public String dropTable(@Valid @RequestBody String dropTable) {
        return dbs.checkQuery(jdbcTemplate, dropTable);
    }

    @PostMapping("/truncate-table")
    public String truncateTable(@Valid @RequestBody String truncateTable) {
        return dbs.checkQuery(jdbcTemplate, truncateTable);
    }

    private String getTableCode(String createString) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(createString);

        if (actualObj.get("tableCode") == null)
            return null;

        return actualObj.get("tableCode").asText();

    }

    private int getNumberOfYears(String createString) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(createString);

        if (actualObj.get("numberOfYears") == null)
            return 5;

        return actualObj.get("numberOfYears").asInt();
    }

    private Map<String, List<String>> getFilters(String createString) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(createString);

        Map<String, List<String>> filterMap = new HashMap<String, List<String>>();

        System.out.println("derperpapraprapr");
        System.out.println(actualObj.get("filters"));
        System.out.println("derperpapraprapr2");

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
