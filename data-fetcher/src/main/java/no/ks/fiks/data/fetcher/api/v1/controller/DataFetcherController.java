package no.ks.fiks.data.fetcher.api.v1.controller;

import no.ks.fiks.data.fetcher.api.v1.service.DataFetcherService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@RequestMapping("/api/v1")
public class DataFetcherController {
    private DataFetcherService dfs;

    public DataFetcherController(JdbcTemplate jdbc) {
        dfs = new DataFetcherService(jdbc);
    }

    @PostMapping("/create-table")
    public String createTable(@Valid @RequestBody String jsonPayload) {
        return dfs.createTable(jsonPayload);
    }

    @PostMapping("/drop-table")
    public String dropTable(@Valid @RequestBody String jsonPayload) {
        return dfs.dropTable(jsonPayload);
    }

    @PostMapping("/truncate-table")
    public String truncateTable(@Valid @RequestBody String jsonPayload) {
        return dfs.truncateTable(jsonPayload);
    }

    @PostMapping("/insert-data")
    public String insertData(@Valid @RequestBody String jsonPayload) {
        return dfs.insertData(jsonPayload);
    }
}
