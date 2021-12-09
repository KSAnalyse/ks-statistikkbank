package no.ks.fiks.database.service.api.v1.controller;

import no.ks.fiks.database.service.api.v1.service.DatabaseService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@RestController
@RequestMapping("/api/v1")
public class DatabaseServiceController {
    private final DatabaseService dbs;

    @Autowired
    public DatabaseServiceController(JdbcTemplate jdbc) {
        dbs = new DatabaseService(jdbc);
    }

    @GetMapping("/status")
    public String runStatus() {
        return "OK";
    }

    /**
     * @param createString the table code of the table to be created
     * @return result of running the create query
     */
    @PostMapping("/create-table")
    public String createTable(@Valid @RequestBody String createString) {
        return dbs.checkQuery(createString);
    }

    /**
     * @param dropTable the table code of the table to be dropped
     * @return result of running the truncate query
     * @see DatabaseService
     */
    @PostMapping("/drop-table")
    public String dropTable(@Valid @RequestBody String dropTable) {
        return dbs.checkQuery(dropTable);
    }

    /**
     * @param truncateTable the table code of the table to be truncated
     * @return result of running the truncate query
     * @see DatabaseService
     */
    @PostMapping("/truncate-table")
    public String truncateTable(@Valid @RequestBody String truncateTable) {
        return dbs.checkQuery(truncateTable);
    }

    @PostMapping("/insert-data")
    public String insertData(@Valid @RequestBody String json) {

        return dbs.convertJsonToInsertQuery(json);
    }


}
