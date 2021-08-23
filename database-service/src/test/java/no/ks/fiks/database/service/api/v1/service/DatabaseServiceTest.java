package no.ks.fiks.database.service.api.v1.service;

import com.sun.xml.bind.v2.TODO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
class DatabaseServiceTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private DatabaseService dbs;
    private String tableName, expectedResultString, inputString;
    private String regexPattern;

    @BeforeEach
    void setUp() {
        dbs = new DatabaseService();
        tableName = "TEST_S.TEST";
        regexPattern = "((?<!\\,|create|drop|truncate|varchar|numeric) (?!varchar|numeric|int|(not )?null))";
        dbs.dropTable(jdbcTemplate, "drop table " + tableName);
    }

    @AfterEach
    void tearDown() {

    }

    @Test
    void testJdbcTemplateNullCheck() {
        assertNotNull(jdbcTemplate);
    }

    @Test
    void testIncorrectSyntaxOnInputString() {
        // Missing a ) at the end to have the correct syntax
        inputString = "create table TEST_S.TEST (Regionkode varchar(100)";
        expectedResultString = "SqlException. Error: 102. Incorrect syntax near ')'.";

        assertEquals(expectedResultString, dbs.runSqlStatement(jdbcTemplate, inputString));
    }

    @Test
    void testCreateNonExistingTable() {
        inputString = "create table TEST_S.TEST (Regionkode varchar(100))";
        expectedResultString = "OK";

        assertEquals(expectedResultString, dbs.runSqlStatement(jdbcTemplate, inputString));
    }

    @Test
    void testCreateExistingTable() {
        inputString = "create table TEST_S.TEST (Regionkode varchar(100))";
        expectedResultString = "SqlException. Error: 2714. There is already an object named 'TEST' in the database.";

        dbs.runSqlStatement(jdbcTemplate, inputString);

        assertEquals(expectedResultString, dbs.runSqlStatement(jdbcTemplate, inputString));
    }

    @Test
    void testDropNonExistingTable() {
        inputString = "drop table TEST_S.TEST";
        expectedResultString = "SqlException. Error: 3701. Cannot drop the table 'TEST_S.TEST'," +
                " because it does not exist or you do not have permission.";

        assertEquals(expectedResultString, dbs.runSqlStatement(jdbcTemplate, inputString));
    }

    @Test
    void testDropExistingTable() {
        inputString = "create table TEST_S.TEST (Regionkode varchar(100))";
        expectedResultString = "OK";

        assertEquals(expectedResultString, dbs.runSqlStatement(jdbcTemplate, inputString));
    }

    @Test
    //TODO: Oppdater til å bruke regex funksjoner i DatabaseService. Endre result til å være evt. return verdi
    void testFirstRegexSplitWithoutSpacesAfterType() {
        String testString = "create table TEST_S.Test (Varchar varchar(255), Integer int, Numeric numeric(18,2))";

        //Splitter på mellomrom som ikke har ',', 'varchar' eller 'numeric' før og ikke en bokstav etter
        String[] result = testString.split(regexPattern);
        assertEquals(3, result.length);
        assertEquals("create table", result[0]);
        assertEquals("TEST_S.Test", result[1]);
        assertEquals("(Varchar varchar(255), Integer int, Numeric numeric(18,2))", result[2]);
    }

    @Test
    //TODO: Oppdater til å bruke regex funksjoner i DatabaseService. Endre result til å være evt. return verdi
    void testFirstRegexSplitWithSpacesAfterType() {
        String testString = "create table TEST_S.Test (Varchar varchar (255), Integer int, Numeric numeric (18,2))";

        //Splitter på mellomrom som ikke har ',', 'varchar' eller 'numeric' før og ikke en bokstav etter
        String[] result = testString.split(regexPattern);

        assertEquals(3, result.length);
        assertEquals("create table", result[0]);
        assertEquals("TEST_S.Test", result[1]);
        assertEquals("(Varchar varchar (255), Integer int, Numeric numeric (18,2))", result[2]);
    }

    @Test
    //TODO: Oppdater til å bruke regex funksjoner i DatabaseService. Endre result til å være evt. return verdi
    void testColumnDefinitionRegexSplitWithoutSpaces() {
        String testString = "Varchar varchar(255), Integer int, Numeric numeric(18,2)";

        //Splitt på mellomrom med og uten komma foran
        String[] result = testString.split(",\\s");

        assertEquals(3, result.length);
        assertEquals("Varchar varchar(255)", result[0]);
        assertEquals("Integer int", result[1]);
        assertEquals("Numeric numeric(18,2)", result[2]);
    }

    @Test
    //TODO: Oppdater til å bruke regex funksjoner i DatabaseService. Endre result til å være evt. return verdi
    void testColumnDefinitionRegexSplitWithSpaces() {
        String testString = "Varchar varchar (255), Integer int, Numeric numeric (18,2)";

        //Splitt på mellomrom med og uten komma foran
        String[] result = testString.split(",\\s");

        assertEquals(3, result.length);
        assertEquals("Varchar varchar (255)", result[0]);
        assertEquals("Integer int", result[1]);
        assertEquals("Numeric numeric (18,2)", result[2]);
    }
}