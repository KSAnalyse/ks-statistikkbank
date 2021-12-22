package no.ks.fiks.data.fetcher.api.v1.service;

import no.ks.fiks.data.fetcher.csvreader.CsvReader;
import no.ks.fiks.data.fetcher.tableinfo.TabellFilter;
import no.ks.fiks.ssbAPI.APIService.SsbApiCall;
import no.ks.fiks.ssbAPI.metadataApi.SsbMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.FileNotFoundException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DataFetcherServiceTest {

    private DataFetcherService dfs;
    private SsbMetadata metadata;
    private List<TabellFilter> tabellFilter;

    @BeforeEach
    void setUp() {
        dfs = new DataFetcherService();
        metadata = new SsbApiCall("05939", 5, null, "131",
                "104", "214", "231", "127").getMetadata();
        CsvReader csvReader = new CsvReader();
        try {
            csvReader.readFromCsv();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        tabellFilter = csvReader.getTablesAndFilters();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testCreateValidTableWithOnlyTableCodeAndSchema() {
        dfs.dropTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\"}");
        assertEquals("OK", dfs.createTable(
                "{\"tableCode\":\"11805\",\"schemaName\":\"ssb\"}"
        ));
    }

    @Test
    void testCreateValidTableWithoutSchema() {
        assertEquals("[ERROR] The json doesn't have the schemaName field.", dfs.createTable(
                "{\"tableCode\":\"11805\"}"
        ));
    }

    @Test
    void testCreateValidTableWithSelectedYears() {
        dfs.dropTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\"}");
        assertEquals("OK", dfs.createTable(
                "{\"tableCode\":\"11805\",\"schemaName\":\"ssb\",\"numberOfYears\":\"5\"}"
        ));
    }

    @Test
    void testCreateValidTableWithAllYears() {
        dfs.dropTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\"}");
        assertEquals("OK", dfs.createTable(
                "{\"tableCode\":\"11805\",\"schemaName\":\"ssb\",\"numberOfYears\":\"-1\"}"
        ));
    }

    @Test
    void testCreateBigValidTable() {
        dfs.dropTable("{\"tableCode\":\"12367\",\"schemaName\":\"ssb\"}");
        assertEquals("OK", dfs.createTable(
                "{\"tableCode\":\"12367\",\"schemaName\":\"ssb\",\"numberOfYears\":\"1\"," +
                        "\"filters\":[{\"code\":\"KOKregnskapsomfa0000\", \"values\":[\"B\"]}]}"
        ));
    }

    @Test
    void testCreateNonExistingTable() {
        assertEquals("[ERROR] Something went wrong while fetching the SsbApiCall data.", dfs.createTable(
                "{\"tableCode\":\"00001\",\"schemaName\":\"ssb\",\"numberOfYears\":\"1\"}"
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
        dfs.createTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfs.dropTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\"}"));
    }

    @Test
    void testDropTableWithoutTableCode() {
        assertEquals("[ERROR] The json doesn't have the tableCode field.",
                dfs.dropTable("{\"schemaName\":\"ssb\"}"));
    }

    @Test
    void testDropTableWithoutSchemaName() {
        assertEquals("[ERROR] The json doesn't have the schemaName field.",
                dfs.dropTable("{\"tableCode\":\"11805\"}"));
    }

    @Test
    void testTruncateTable() {
        dfs.createTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfs.truncateTable("{\"tableCode\":\"11805\",\"schemaName\":\"ssb\"}"));
    }

    @Test
    void testTruncateTableWithoutTableCode() {
        assertEquals("[ERROR] The json doesn't have the tableCode field.",
                dfs.truncateTable("{\"schemaName\":\"ssb\"}"));
    }

    @Test
    void testTruncateTableWithoutSchemaName() {
        assertEquals("[ERROR] The json doesn't have the schemaName field.",
                dfs.truncateTable("{\"tableCode\":\"11805\"}"));
    }

    @Test
    void testInsertSingleYear() {
        dfs.createTable("{\"tableCode\":\"11814\",\"schemaName\":\"ssb\",\"numberOfYears\":\"5\"}");
        assertEquals("OK", dfs.insertData(
                "{\"tableCode\":\"11814\",\"schemaName\":\"ssb\",\"numberOfYears\":\"1\"," +
                        "\"filters\":[{\"code\":\"KOKkommuneregion0000\", \"values\":[\"0301\"]}]}"
        ));
    }

    @Test
    void testInsertWithoutTableCode() {
        assertEquals("[ERROR] The json doesn't have the tableCode field.", dfs.insertData(
                "{\"schemaName\":\"ssb\"}"
        ));
    }

    @Test
    void testInsertWithoutSchemaName() {
        assertEquals("[ERROR] The json doesn't have the schemaName field.", dfs.insertData(
                "{\"tableCode\":\"11814\"}"
        ));
    }

    @Test
    void testInsertWithoutNumberOfYears() {
        dfs.createTable("{\"tableCode\":\"11814\",\"schemaName\":\"ssb\"}");
        assertEquals("OK", dfs.insertData(
                "{\"tableCode\":\"11814\",\"schemaName\":\"ssb\"," +
                        "\"filters\":[{\"code\":\"KOKkommuneregion0000\", \"values\":[\"0301\"]}]}"
        ));
    }

    @Test
    void testInsertWithInvalidTable() {
        dfs.createTable("{\"tableCode\":\"00001\",\"schemaName\":\"ssb\",\"numberOfYears\":\"1\"}");
        assertEquals("[ERROR] Failed while fetching SsbApiCall data.", dfs.insertData(
                "{\"tableCode\":\"00001\",\"schemaName\":\"ssb\",\"numberOfYears\":\"1\"," +
                        "\"filters\":[{\"code\":\"KOKkommuneregion0000\", \"values\":[\"0301\"]}]}"
        ));
    }

    @Test
    void testInsertWithInvalidApiTable() {
        dfs.createTable("{\"tableCode\":\"11211\",\"schemaName\":\"ssb\",\"numberOfYears\":\"1\"}");
        assertEquals("[ERROR] Failed while fetching and structuring data.", dfs.insertData(
                "{\"tableCode\":\"11211\",\"schemaName\":\"ssb\",\"numberOfYears\":\"1\"," +
                        "\"filters\":[{\"code\":\"KOKkommuneregion0000\", \"values\":[\"0301\"]}]}"
        ));
    }

    @Test
    void testCreateColumnDeclarations() {
        Map<String, List<String>> filters = new LinkedHashMap<>();
        for (TabellFilter table : tabellFilter) {
            if (table.getTabellnummer().equals("05939"))
                filters = table.getLagTabellFilter();
        }

        System.out.println(filters);
        System.out.println(dfs.createColumnDeclarations(metadata, filters));
    }
}