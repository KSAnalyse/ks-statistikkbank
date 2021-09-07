package no.ks.fiks.database.service.api.v1.service;

import no.ks.fiks.database.service.api.v1.config.SqlConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DatabaseServiceTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private SqlConfiguration sqlConfig;

    private DatabaseService dbs;
    private String validTableName, validSchemaName, expectedResultString, inputString, validDest;
    private String syntaxRegexPattern, commandRegexPattern;

    @BeforeEach
    void setUp() {
        dbs = new DatabaseService(sqlConfig);
        validSchemaName = sqlConfig.getSchemaName();
        validTableName = "toto";

        validDest = validSchemaName + "." + validTableName;

        syntaxRegexPattern = "((?<!\\,|create|drop|truncate|varchar|numeric) (?!varchar|numeric|int|(not )?null))";
        commandRegexPattern = "(create|drop|truncate) table";

        dbs.runSqlStatement(jdbcTemplate, "drop table " + validDest);
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
        inputString = "create table " + validDest + " (Regionkode varchar(100)";
        expectedResultString = "SQL Error: 102. Incorrect syntax near ')'.";

        assertEquals(expectedResultString, dbs.runSqlStatement(jdbcTemplate, inputString));
    }

    @Test
    void testValidNameStructure() {
        assertTrue(dbs.checkValidTableName(validSchemaName + "." + validTableName));
    }

    @Test
    void testValidDropTable() {
        dbs.runSqlStatement(jdbcTemplate, "create table " + validDest + "([Regionkode] int)");

        assertEquals("OK", dbs.checkSqlStatement(jdbcTemplate, "drop table " + validDest));
    }

    @Test
    void testDropTableWithInvalidNameStructure() {
        dbs.runSqlStatement(jdbcTemplate, "create table " + validDest + "([Regionkode] int)");

        assertEquals("Not a valid structure on query.", dbs.checkSqlStatement(jdbcTemplate, "drop table test.test teststt sfdsf"));
    }

    @Test
    void testDropNonExistingTable() {
        String expectedErrorMessage = "SQL Error: 3701. Cannot drop the table '" + validDest +
                "', because it does not exist or you do not have permission.";

        assertEquals(expectedErrorMessage, dbs.checkSqlStatement(jdbcTemplate, "drop table " + validDest));
    }

    @Test
    void testCreateValidTable() {
        String sqlQuery = "create table " + validDest + " ([Col1] [varchar] (200))";

        assertEquals("OK", dbs.checkSqlStatement(jdbcTemplate, sqlQuery));
    }

    @Test
    void testInvalidNameStructure() {
        assertFalse(dbs.checkValidTableName("test.test teststt sfdsf"));
    }

    @Test
    void testDestinationNameWithSeveralDots() {
        assertFalse(dbs.checkValidTableName(validSchemaName + "." + validTableName + "." + validTableName));
    }

    @Test
    void testInvalidSchemaName() {
        assertFalse(dbs.checkValidTableName("dbo." + validTableName));
    }

    @Test
    void testUseOfProtectedWord() {
        assertFalse(dbs.checkValidTableName(validSchemaName + "." + "SELECT"));
        assertFalse(dbs.checkValidTableName(validSchemaName + "." + "select"));
    }

    //TODO: Fiks opp denne til nye strukturen
    /*
    @Test
    void testCreateNonExistingTable() {
        inputString = "create table TEST_S.TEST (Regionkode varchar(100))";
        expectedResultString = "OK";

        assertEquals(expectedResultString, dbs.runSqlStatement(jdbcTemplate, inputString));
    }

     */

    //TODO: Fiks opp denne til nye strukturen
    /*
    @Test
    void testCreateExistingTable() {
        inputString = "create table TEST_S.TEST (Regionkode varchar(100))";
        expectedResultString = "SqlException. Error: 2714. There is already an object named 'TEST' in the database.";

        dbs.runSqlStatement(jdbcTemplate, inputString);

        assertEquals(expectedResultString, dbs.runSqlStatement(jdbcTemplate, inputString));
    }
    */

    //TODO: Fiks opp denne til nye strukturen
    /*
    @Test
    void testDropNonExistingTable() {
        inputString = "drop table ksssb.TEST";
        expectedResultString = "SqlException. Error: 3701. Cannot drop the table 'TEST_S.TEST'," +
                " because it does not exist or you do not have permission.";

        assertEquals(expectedResultString, dbs.runSqlStatement(jdbcTemplate, inputString));
    }

     */

    //TODO: Fiks opp denne til nye strukturen
    /*
    @Test
    void testDropExistingTable() {
        inputString = "create table TEST_S.TEST (Regionkode varchar(100))";
        expectedResultString = "OK";

        assertEquals(expectedResultString, dbs.runSqlStatement(jdbcTemplate, inputString));
    }
     */

    @Test
    //TODO: Oppdater til å bruke regex funksjoner i DatabaseService. Endre result til å være evt. return verdi
    void testFirstRegexSplitWithoutSpacesAfterType() {
        String testString = "create table TEST_S.Test (Varchar varchar(255), Integer int, Numeric numeric(18,2))";

        //Splitter på mellomrom som ikke har ',', 'varchar' eller 'numeric' før og ikke en bokstav etter
        String[] result = testString.split(syntaxRegexPattern);
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
        String[] result = testString.split(syntaxRegexPattern);

        assertEquals(3, result.length);
        assertEquals("create table", result[0]);
        assertEquals("TEST_S.Test", result[1]);
        assertEquals("(Varchar varchar (255), Integer int, Numeric numeric (18,2))", result[2]);
    }

    @Test
    void testValidCommand() {
        String createString, dropString, truncateString;

        createString= "create table";
        dropString = "drop table";
        truncateString = "truncate table";

        assertTrue(createString.matches(commandRegexPattern));
        assertTrue(dropString.matches(commandRegexPattern));
        assertTrue(truncateString.matches(commandRegexPattern));
    }

    @Test
    void testNonValidCommand() {
        String createString, dropString, insertString, patternAtEndString, patternAtBeginningString;

        createString= "create tabl";
        dropString = "dro table";
        insertString = "insert into";

        patternAtBeginningString = "create table test test2 test3";
        patternAtEndString = "test create table";

        assertFalse(createString.matches(commandRegexPattern));
        assertFalse(dropString.matches(commandRegexPattern));
        assertFalse(insertString.matches(commandRegexPattern));

        assertFalse(patternAtBeginningString.matches(commandRegexPattern));
        assertFalse(patternAtEndString.matches(commandRegexPattern));
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