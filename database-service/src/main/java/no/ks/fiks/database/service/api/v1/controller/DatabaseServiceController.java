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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/api/v1")
public class DatabaseServiceController {

    /**
     * @param createString the table code of the table to be created
     * @return result of running the create query
     */
    @PostMapping("/create-table")
    public String createTable(@Valid @RequestBody String createString) {
        return null;
    }

    /**
     * @param dropTable the table code of the table to be dropped
     * @return result of running the truncate query
     * @see DatabaseService
     */
    @PostMapping("/drop-table")
    public String dropTable(@Valid @RequestBody String dropTable) {
        String dropQuery = String.format("drop table %s.[SSB_%s]", config.getSchemaName(), dropTable);
        return dbs.checkQuery(jdbcTemplate, dropQuery);
    }

    /**
     * @param truncateTable the table code of the table to be truncated
     * @return result of running the truncate query
     * @see DatabaseService
     */
    @PostMapping("/truncate-table")
    public String truncateTable(@Valid @RequestBody String truncateTable) {
        String truncateQuery = String.format("truncate table %s.[SSB_%s]", config.getSchemaName(), truncateTable);
        return dbs.checkQuery(jdbcTemplate, truncateQuery);
    }


}
