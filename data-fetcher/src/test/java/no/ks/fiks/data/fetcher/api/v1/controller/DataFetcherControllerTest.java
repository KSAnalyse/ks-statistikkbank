package no.ks.fiks.data.fetcher.api.v1.controller;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DataFetcherControllerTest {
    @Autowired
    private JdbcTemplate jdbc;

    private DataFetcherController dfc;

    @BeforeEach
    void setUp() {
        dfc = new DataFetcherController(jdbc);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void createValidTableWithOnlyTableCode() {
        dfc.dropTable("{\"tableCode\":\"11814\"}");
        assertEquals("OK", dfc.createTable(
                "{\"tableCode\":\"11814\"}"
        ));
    }

    @Test
    void createValidTableWithOnlyYears() {
        dfc.dropTable("{\"tableCode\":\"11805\"}");
        assertEquals("OK", dfc.createTable(
                "{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}"
        ));
    }

    @Test
    void createValidTableWithAllYears() {
        dfc.dropTable("{\"tableCode\":\"11805\"}");
        assertEquals("OK", dfc.createTable(
                "{\"tableCode\":\"11805\",\"numberOfYears\":\"-1\"}"
        ));
    }

    @Test
    void createBigValidTable() {
        dfc.dropTable("{\"tableCode\":\"12367\"}");
        assertEquals("OK", dfc.createTable(
                "{\"tableCode\":\"12367\",\"numberOfYears\":\"1\", \"filters\":[{\"code\":\"KOKregnskapsomfa0000\", \"values\":[\"B\"]}]}"
        ));
    }

    @Test
    void createNonExistingTable() {
        assertEquals("[ERROR] Something went wrong while fetching the SsbApiCall data.", dfc.createTable(
                "{\"tableCode\":\"00001\",\"numberOfYears\":\"1\"}"
        ));
    }

    @Test
    void createTableWithEmptyJsonExistingTable() {
        assertEquals("[ERROR] The json doesn't have the tableCode field.", dfc.createTable(
                "{}"
        ));
    }

    @Test
    void dropTable() {
        dfc.createTable("{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfc.dropTable("{\"tableCode\":\"11805\"}"));
    }

    @Test
    void truncateTable() {
        dfc.createTable("{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfc.truncateTable("{\"tableCode\":\"11805\"}"));
    }
}