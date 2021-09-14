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
    private String validTableName, validSchemaName, expectedResultString, inputString, validDest,
            validPersistingTableName, validPersistingDest;
    private String validPersistingTableNameMultipleColumns;
    private String validPersistingDestMultipleColumn;

    @BeforeEach
    void setUp() {
        dbs = new DatabaseService(sqlConfig);
        validSchemaName = sqlConfig.getSchemaName();
        validTableName = "toto";
        validPersistingTableName = "one_column";
        validPersistingTableNameMultipleColumns = "multiple_column";

        validDest = validSchemaName + "." + validTableName;
        validPersistingDest = validSchemaName + "." + validPersistingTableName;
        validPersistingDestMultipleColumn = validSchemaName + "." + validPersistingTableNameMultipleColumns;
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

        dbs.runSqlStatement(jdbcTemplate, "drop table " + validDest);
    }

    @Test
    void testDropTableWithInvalidNameStructure() {
        dbs.runSqlStatement(jdbcTemplate, "create table " + validDest + "([Regionkode] int)");

        assertEquals("Not a valid structure on query.", dbs.checkSqlStatement(jdbcTemplate, "drop table test.test teststt sfdsf"));

        dbs.runSqlStatement(jdbcTemplate, "drop table " + validDest);
    }

    @Test
    void testDropNonExistingTable() {
        String expectedErrorMessage = "SQL Error: 3701. Cannot drop the table '" + validDest +
                "', because it does not exist or you do not have permission.";

        assertEquals(expectedErrorMessage, dbs.checkSqlStatement(jdbcTemplate, "drop table " + validDest));
    }

    @Test
    void testCreateValidTableOneColumn() {
        String sqlQuery = "create table " + validPersistingDest + " ([Col1] [varchar] (200))";

        dbs.runSqlStatement(jdbcTemplate, "drop table " + validPersistingDest);

        assertEquals("OK", dbs.checkSqlStatement(jdbcTemplate, sqlQuery));
    }

    @Test
    void testCreateValidTableMultipleColumns() {
        String sqlQuery = "create table " + validPersistingDestMultipleColumn + " ([Regionkode] [varchar] (200), [En eller annen int] [int], [Verdi] [numeric] (18,2))";

        dbs.runSqlStatement(jdbcTemplate, "drop table " + validPersistingDestMultipleColumn);

        assertEquals("OK", dbs.checkSqlStatement(jdbcTemplate, sqlQuery));
    }

    @Test
    void testCreateTableWithInvalidVarcharSize() {
        String sqlQuery = "create table " + validDest + " ([Col1] [varchar] (1001))";

        dbs.runSqlStatement(jdbcTemplate, "drop table " + validDest);

        assertEquals("Not a valid column declaration.", dbs.checkSqlStatement(jdbcTemplate, sqlQuery));
    }

    @Test
    void testCreateInvalidTableOneColumn() {
        String sqlQuery = "create table " + validDest + " ([Col1] [varchar (200))";

        dbs.runSqlStatement(jdbcTemplate, "drop table " + validDest);

        assertEquals("Not a valid structure on query.", dbs.checkSqlStatement(jdbcTemplate, sqlQuery));
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
}