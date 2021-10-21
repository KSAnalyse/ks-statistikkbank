package no.ks.fiks.data.fetcher.api.v1.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DataFetcherServiceTest {

    @Autowired
    private JdbcTemplate jdbc;

    private DataFetcherService dfs;

    @BeforeEach
    void setUp() {
        dfs = new DataFetcherService(jdbc);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void createValidTableWithOnlyTableCode() {
        dfs.dropTable("{\"tableCode\":\"11814\"}");
        assertEquals("OK", dfs.createTable(
                "{\"tableCode\":\"11814\"}"
        ));
    }

    @Test
    void createValidTableWithOnlyYears() {
        dfs.dropTable("{\"tableCode\":\"11805\"}");
        assertEquals("OK", dfs.createTable(
                "{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}"
        ));
    }

    @Test
    void createValidTableWithAllYears() {
        dfs.dropTable("{\"tableCode\":\"11805\"}");
        assertEquals("OK", dfs.createTable(
                "{\"tableCode\":\"11805\",\"numberOfYears\":\"-1\"}"
        ));
    }

    @Test
    void createBigValidTable() {
        dfs.dropTable("{\"tableCode\":\"12367\"}");
        assertEquals("OK", dfs.createTable(
                "{\"tableCode\":\"12367\",\"numberOfYears\":\"1\", \"filters\":[{\"code\":\"KOKregnskapsomfa0000\", \"values\":[\"B\"]}]}"
        ));
    }

    @Test
    void createNonExistingTable() {
        assertEquals("[ERROR] Something went wrong while fetching the SsbApiCall data.", dfs.createTable(
                "{\"tableCode\":\"00001\",\"numberOfYears\":\"1\"}"
        ));
    }

    @Test
    void createTableWithEmptyJsonExistingTable() {
        assertEquals("[ERROR] The json doesn't have the tableCode field.", dfs.createTable(
                "{}"
        ));
    }

    @Test
    void dropTable() {
        dfs.createTable("{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfs.dropTable("{\"tableCode\":\"11805\"}"));
    }

    @Test
    void truncateTable() {
        dfs.createTable("{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfs.truncateTable("{\"tableCode\":\"11805\"}"));
    }
}