package no.ks.fiks.data.fetcher.api.v1.controller;

import no.ks.fiks.data.fetcher.api.v1.service.DataFetcherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@RestController
@RequestMapping("/api/v1a")
public class DataFetcherController {
    private final DataFetcherService dfs;

    @Autowired
    public DataFetcherController(DataFetcherService dfs) {
        this.dfs = dfs;
    }

    @GetMapping("/status")
    public String status() {
        return "OK";
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
