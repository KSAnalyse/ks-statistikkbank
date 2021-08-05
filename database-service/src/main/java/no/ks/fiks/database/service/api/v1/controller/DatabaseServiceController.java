package no.ks.fiks.database.service.api.v1.controller;

import no.ks.fiks.database.service.api.v1.service.DatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@RestController
@RequestMapping("/api/v1")
public class DatabaseServiceController {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private DatabaseService dbs = new DatabaseService();

    @GetMapping("/test")
    public String test() {
        return "yep";
    }

    @PostMapping("/create-table")
    public String createTable(@Valid @RequestBody String createString) {
        return dbs.createTable(jdbcTemplate, createString);
    }

    @PostMapping("/drop-table")
    public String dropTable(@Valid @RequestBody String dropTable) {
        return dbs.dropTable(jdbcTemplate, dropTable);
    }
}
