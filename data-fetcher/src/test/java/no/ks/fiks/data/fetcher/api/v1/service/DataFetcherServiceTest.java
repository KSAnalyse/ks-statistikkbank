package no.ks.fiks.data.fetcher.api.v1.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DataFetcherServiceTest {

    private DataFetcherService dfs;

    @BeforeEach
    void setUp() {
        dfs = new DataFetcherService();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testCreateValidTableWithOnlyTableCode() {
        dfs.dropTable("{\"tableCode\":\"11805\"}");
        assertEquals("OK", dfs.createTable(
                "{\"tableCode\":\"11805\"}"
        ));
    }

    @Test
    void testCreateValidTableWithOnlyYears() {
        dfs.dropTable("{\"tableCode\":\"11805\"}");
        assertEquals("OK", dfs.createTable(
                "{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}"
        ));
    }

    @Test
    void testCreateValidTableWithAllYears() {
        dfs.dropTable("{\"tableCode\":\"11805\"}");
        assertEquals("OK", dfs.createTable(
                "{\"tableCode\":\"11805\",\"numberOfYears\":\"-1\"}"
        ));
    }

    @Test
    void testCreateBigValidTable() {
        dfs.dropTable("{\"tableCode\":\"12367\"}");
        assertEquals("OK", dfs.createTable(
                "{\"tableCode\":\"12367\",\"numberOfYears\":\"1\", \"filters\":[{\"code\":\"KOKregnskapsomfa0000\", \"values\":[\"B\"]}]}"
        ));
    }

    @Test
    void testCreateNonExistingTable() {
        assertEquals("[ERROR] Something went wrong while fetching the SsbApiCall data.", dfs.createTable(
                "{\"tableCode\":\"00001\",\"numberOfYears\":\"1\"}"
        ));
    }

    @Test
    void testCreateTableWithEmptyJsonExistingTable() {
        assertEquals("[ERROR] The json doesn't have the tableCode field.", dfs.createTable(
                "{}"
        ));
    }

    @Test
    void testDropTable() {
        dfs.createTable("{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfs.dropTable("{\"tableCode\":\"11805\"}"));
    }

    @Test
    void testTruncateTable() {
        dfs.createTable("{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfs.truncateTable("{\"tableCode\":\"11805\"}"));
    }

    @Test
    void testInsertSingleYear() {
        dfs.createTable("{\"tableCode\":\"11814\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfs.insertData(
                "{\"tableCode\":\"11814\",\"numberOfYears\":\"1\", \"filters\":[{\"code\":\"KOKkommuneregion0000\", \"values\":[\"0301\"]}]}"
        ));
    }
}