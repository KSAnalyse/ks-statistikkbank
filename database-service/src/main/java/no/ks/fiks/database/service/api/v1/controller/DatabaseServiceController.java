package no.ks.fiks.database.service.api.v1.controller;

import no.ks.fiks.database.service.api.v1.config.SqlConfiguration;
import no.ks.fiks.database.service.api.v1.service.DatabaseService;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class DatabaseServiceController {
    private final JdbcTemplate jdbcTemplate;
    private final SqlConfiguration config;
    private final DatabaseService dbs;

    public DatabaseServiceController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        config = new SqlConfiguration();
        dbs = new DatabaseService(config);
    }

    /**
     * @param createString the table code of the table to be created
     * @return result of running the create query
     */
    @PostMapping("/create-table")
    public String createTable(@Valid @RequestBody String createString) {
        return dbs.checkQuery(jdbcTemplate, createString);
    }

    /**
     * @param dropTable the table code of the table to be dropped
     * @return result of running the truncate query
     * @see DatabaseService
     */
    @PostMapping("/drop-table")
    public String dropTable(@Valid @RequestBody String dropTable) {
        return dbs.checkQuery(jdbcTemplate, dropTable);
    }

    /**
     * @param truncateTable the table code of the table to be truncated
     * @return result of running the truncate query
     * @see DatabaseService
     */
    @PostMapping("/truncate-table")
    public String truncateTable(@Valid @RequestBody String truncateTable) {
        return dbs.checkQuery(jdbcTemplate, truncateTable);
    }

    public String insertData(@Valid @RequestBody List<Map<String[], BigDecimal>> dataResult,
                             String tableName) {

        return dbs.insertData(jdbcTemplate, dataResult, tableName);
    }


}
