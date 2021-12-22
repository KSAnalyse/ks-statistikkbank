package no.ks.fiks.data.fetcher.api.v1.controller;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DataFetcherControllerTest {
    private DataFetcherController dfc;

    @BeforeEach
    void setUp() {
        //dfc = new DataFetcherController();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testCreateValidSsbTableWithOnlyTableCodeAndSchema() {
        dfc.dropTable("{\"tableCode\":\"11814\",\"schemaName\":\"ssb\"}");
        assertEquals("OK", dfc.createTable(
                "{\"tableCode\":\"11814\",\"schemaName\":\"ssb\"}"
        ));
    }

    @Test
    void testCreateValidKsTableWithOnlyTableCodeAndSchema() {
        dfc.dropTable("{\"tableCode\":\"11814\",\"schemaName\":\"ks\"}");
        assertEquals("OK", dfc.createTable(
                "{\"tableCode\":\"11814\",\"schemaName\":\"ks\"}"
        ));
    }

    @Test
    void testCreateValidTableWithOnlyYears() {
        dfc.dropTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\"}");
        assertEquals("OK", dfc.createTable(
                "{\"tableCode\":\"11805\",\"schemaName\":\"ssb\",\"numberOfYears\":\"5\"}"
        ));
    }

    @Test
    void testCreateValidTableWithAllYears() {
        dfc.dropTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\"}");
        assertEquals("OK", dfc.createTable(
                "{\"tableCode\":\"11805\",\"schemaName\":\"ssb\",\"numberOfYears\":\"-1\"}"
        ));
    }

    @Test
    void testCreateBigValidTable() {
        dfc.dropTable("{\"tableCode\":\"12367\",\"schemaName\":\"ssb\"}");
        assertEquals("OK", dfc.createTable(
                "{\"tableCode\":\"12367\",\"schemaName\":\"ssb\",\"numberOfYears\":\"1\", \"filters\":[{\"code\":\"KOKregnskapsomfa0000\", \"values\":[\"B\"]}]}"
        ));
    }

    @Test
    void testCreateNonExistingTable() {
        assertEquals("[ERROR] Something went wrong while fetching the SsbApiCall data.", dfc.createTable(
                "{\"tableCode\":\"00001\",\"schemaName\":\"ssb\",\"numberOfYears\":\"1\"}"
        ));
    }

    @Test
    void testCreateTableWithEmptyJsonExistingTable() {
        assertEquals("[ERROR] The json doesn't have the tableCode field.", dfc.createTable(
                "{}"
        ));
    }

    @Test
    void testDropTable() {
        dfc.createTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfc.dropTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\"}"));
    }

    @Test
    void testTruncateTable() {
        dfc.createTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfc.truncateTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\"}"));
    }

    @Test
    void testInsertSingleYear() {
        dfc.createTable("{\"tableCode\":\"11814\",\"schemaName\":\"ssb\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfc.insertData(
                "{\"tableCode\":\"11814\",\"schemaName\":\"ssb\",\"numberOfYears\":\"1\"," +
                        "\"filters\":[{\"code\":\"KOKkommuneregion0000\", \"values\":[\"0301\"]}]}"
        ));
    }
}