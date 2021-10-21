package no.ks.fiks.data.fetcher.api.v1.controller;

import no.ks.fiks.database.service.api.v1.config.SqlConfiguration;
import no.ks.fiks.database.service.api.v1.controller.DatabaseServiceController;
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
    private DatabaseServiceController dbsc;

    @BeforeEach
    void setUp() {
        sqlConfig = new SqlConfiguration();
        dbs = new DatabaseService(sqlConfig);
        dfc = new DataFetcherController(jdbc);
        dbsc = new DatabaseServiceController(jdbc);
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
    void createValidTableWithAllYears() {
        assertEquals("OK", dbsc.createTable(
                "{\"tableCode\":\"11805\",\"numberOfYears\":\"-1\"}"
        ));
    }

    @Test
    void createBigValidTable() {
        assertEquals("OK", dbsc.createTable(
                "{\"tableCode\":\"12367\",\"numberOfYears\":\"1\", \"filters\":[{\"code\":\"KOKregnskapsomfa0000\", \"values\":[\"B\"]}]}"
        ));
    }

    @Test
    void createNonExistingTable() {
        assertEquals("[ERROR] Status code from exception: 400", dbsc.createTable(
                "{\"tableCode\":\"00001\",\"numberOfYears\":\"1\"}"
        ));
    }

    @Test
    void createTableWithEmptyJsonExistingTable() {
        assertEquals("[ERROR] The json doesn't have the tableCode field.", dbsc.createTable(
                "{}"
        ));
    }

    @Test
    void dropTable() {
        dbsc.createTable("{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dbsc.dropTable("11805"));
    }

    @Test
    void truncateTable() {
        dbsc.createTable("{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dbsc.truncateTable("11805"));
    }

    @Test
    void createTable() {
    }

    @Test
    void insertData() {
    }
}