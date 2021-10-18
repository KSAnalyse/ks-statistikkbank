package no.ks.fiks.data.fetcher.api.v1.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@RequestMapping("/api/v1")
public class DataFetcherController {

    @PostMapping("/create-table")
    public String createTable(@Valid @RequestBody String jsonPayload) {
        return null;
    }

    @PostMapping("/drop-table")
    public String dropTable(@Valid @RequestBody String jsonPayload) {
        return null;
    }

    @PostMapping("/truncate-table")
    public String truncateTable(@Valid @RequestBody String jsonPayload) {
        return null;
    }

    @PostMapping("/insert-data")
    public String insertData(@Valid @RequestBody String jsonPayload) {
        return null;
    }
}
