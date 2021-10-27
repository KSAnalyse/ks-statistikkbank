package no.ks.fiks.database.service.api.v1.controller;

import no.ks.fiks.database.service.api.v1.config.SqlConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DatabaseServiceControllerTest {

    @Autowired
    private DatabaseServiceController dbsc;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private SqlConfiguration sqlConfig;

    private String validDest;
    private URL url;

    @BeforeEach
    void setUp() throws MalformedURLException {
        //dbsc = new DatabaseServiceController(jdbc);

        validDest = sqlConfig.getSchemaName() + ".toto";
        url = new URL("http://localhost:8080/api/v1/create-table");
    }

    @AfterEach
    void tearDown() {
        dbsc.dropTable("drop table " + validDest);
    }

    @Test
    void testJdbcTemplateNullCheck() {
        assertNotNull(jdbc);
    }

    @Test
    void testCreateValidTableOneColumn() throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json; utf-8");
        connection.setRequestProperty("Accept", "application/json");
        connection.setDoOutput(true);

        String test = "test";

        try(OutputStream os = connection.getOutputStream()) {
            byte[] input = test.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        try(BufferedReader br = new BufferedReader(
                new InputStreamReader(connection.getInputStream(), "utf-8"))) {
            StringBuilder response = new StringBuilder();
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            System.out.println(response.toString());
        }

        connection.disconnect();
        //connection.setRequestMethod("POST");
        //connection.set
        //String sqlQuery = "create table " + validDest + " ([Col1] [varchar] (200))";

        //assertEquals("OK", dbsc.createTable(sqlQuery));
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