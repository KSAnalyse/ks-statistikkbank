package no.ks.fiks.data.fetcher.api.v1.controller;

import no.ks.fiks.database.service.api.v1.config.SqlConfiguration;
import no.ks.fiks.database.service.api.v1.service.DatabaseService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.*;

class DataFetcherControllerTest {
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private SqlConfiguration sqlConfig;

    private DatabaseService dbs;
    private DataFetcherController dfc;

    @BeforeEach
    void setUp() {
        dbs = new DatabaseService(sqlConfig);
        dfc = new DataFetcherController(jdbc);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void createValidTableWithOnlyTableCode() {
        assertEquals("OK", dfc.createTable(
                "{\"tableCode\":\"11814\"}"
        ));
    }

    @Test
    void createValidTableWithOnlyYears() {
        assertEquals("OK", dfc.createTable(
                "{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}"
        ));
    }

    @Test
    void createTable() {
    }

    @Test
    void dropTable() {
    }

    @Test
    void truncateTable() {
    }

    @Test
    void insertData() {
    }
}