package no.ks.fiks.database.service.api.v1.controller;

import no.ks.fiks.database.service.api.v1.config.SqlConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.net.MalformedURLException;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DatabaseServiceControllerTest {

    @Autowired
    private DatabaseServiceController dbsc;

    @Autowired
    private SqlConfiguration sqlConfig;

    private String validDest;

    @BeforeEach
    void setUp() throws MalformedURLException {
        validDest = "ks.toto";
    }

    @AfterEach
    void tearDown() {
        dbsc.dropTable("drop table " + validDest);
    }

    @Test
    void testDatabaseServiceController() {assertNotNull(dbsc);}

    @Test
    void testCreateValidTableWithOnlyTableCode() {
        assertEquals("OK", dbsc.createTable(
                "create table " + validDest + " ([Regionkode] [varchar] (5))"
        ));
    }

    /*
    @Test
    void createValidTableWithOnlyTableCode() {
        assertEquals("OK", dbsc.createTable(
                "{\"tableCode\":\"11814\"}"
        ));
    }

     */

    /*
    @Test
    void createValidTableWithOnlyYears() {
        assertEquals("OK", dbsc.createTable(
                "{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}"
        ));
    }

     */
    /*
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
*/
    /*
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
    */
}