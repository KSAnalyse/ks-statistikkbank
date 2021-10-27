package no.ks.fiks.database.service.api.v1.service;

import no.ks.fiks.database.service.api.v1.config.SqlConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;


import java.math.BigDecimal;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DatabaseServiceTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private SqlConfiguration sqlConfig;

    private DatabaseService dbs;
    private String validTableName;
    private String validSchemaName;
    private String validDest;
    private String validPersistingDest;
    private String validPersistingDestMultipleColumn;

    @BeforeEach
    void setUp() {
        String validPersistingTableName = "one_column";
        String validPersistingTableNameMultipleColumns = "multiple_column";
        dbs = new DatabaseService(jdbcTemplate);
        validSchemaName = sqlConfig.getSchemaName();
        validTableName = "toto";

        validDest = validSchemaName + "." + validTableName;
        validPersistingDest = validSchemaName + "." + validPersistingTableName;
        validPersistingDestMultipleColumn = validSchemaName + "." + validPersistingTableNameMultipleColumns;
    }

    @AfterEach
    void tearDown() {
        dbs.checkQuery("drop table " + validDest);
    }

    @Test
    void testJdbcTemplateNullCheck() {
        assertNotNull(jdbcTemplate);
    }

    @Test
    void testIncorrectSyntaxOnInputString() {
        // Missing a ) at the end to have the correct syntax
        String inputString = "create table " + validDest + " (Regionkode varchar(100)";
        String expectedResultString = "[ERROR] Not a valid structure on query \"create table ssbks.toto (Regionkode varchar(100)\".";

        assertEquals(expectedResultString, dbs.checkQuery(inputString));
    }

    @Test
    void testValidNameStructure() {
        assertTrue(dbs.checkValidTableName(validSchemaName + "." + validTableName));
    }

    @Test
    void testValidDropTable() {
        assertEquals("OK", dbs.checkQuery("create table " + validDest + " ([Regionkode] [int])"));

        assertEquals("OK", dbs.checkQuery("drop table " + validDest));
    }

    @Test
    void testDropTableWithInvalidNameStructure() {
        assertEquals("OK", dbs.checkQuery("create table " + validDest + " ([Regionkode] [int])"));

        assertEquals("[ERROR] Not a valid structure on query \"drop table test.test teststt sfdsf\".", dbs.checkQuery("drop table test.test teststt sfdsf"));
    }

    @Test
    void testDropNonExistingTable() {
        String expectedErrorMessage = "SQL Error: 3701. Cannot drop the table '" + validDest +
                "', because it does not exist or you do not have permission.";

        assertEquals(expectedErrorMessage, dbs.checkQuery("drop table " + validDest));
    }

    @Test
    void testCreateValidTableOneColumn() {
        String sqlQuery = "create table " + validDest + " ([Col1] [varchar] (200))";

        dbs.checkQuery("drop table " + validDest);

        assertEquals("OK", dbs.checkQuery(sqlQuery));
    }

    @Test
    void testCreateValidTableMultipleColumns() {
        String sqlQuery = "create table " + validDest + " ([Regionkode] [varchar] (200), [En eller annen int] [int], [Verdi] [numeric] (18,2))";

        dbs.checkQuery("drop table " + validDest);

        assertEquals("OK", dbs.checkQuery(sqlQuery));
    }

    @Test
    void testCreateTableWithInvalidVarcharSize() {
        String sqlQuery = "create table " + validDest + " ([Col1] [varchar] (1001))";

        dbs.checkQuery("drop table " + validDest);

        assertEquals("Not a valid column declaration.", dbs.checkQuery(sqlQuery));
    }

    @Test
    void testCreateInvalidTableOneColumn() {
        String sqlQuery = "create table " + validDest + " ([Col1] [varchar (200))";

        dbs.checkQuery("drop table " + validDest);

        assertEquals("[ERROR] Not a valid structure on query \"create table ssbks.toto ([Col1] [varchar (200))\".", dbs.checkQuery(sqlQuery));
    }

    @Test
    void testInvalidSchemaName() {
        assertFalse(dbs.checkValidTableName("dbo." + validTableName));
    }

    @Test
    void testUseOfProtectedWordInTableName() {
        assertFalse(dbs.checkValidTableName(validSchemaName + "." + "SELECT"));
        assertFalse(dbs.checkValidTableName(validSchemaName + "." + "select"));
    }

    @Test
    void testUseOfProtectedWordInColumnName() {
        String sqlQuery = "create table " + validDest + " ([insert] [varchar] (200))";

        assertEquals("Not a valid column declaration.", dbs.checkQuery(sqlQuery));
    }

    @Test
    void testDropTableIllegalSchemaName() {
        String sqlQuery = "drop table [derpderp].[SSB_12356]";

        assertEquals("Not a valid destination name.", dbs.checkQuery(sqlQuery));
    }

    @Test
    void testCreateTableIllegalSchemaName() {
        String sqlQuery = "create table [derpderp].[SSB_12356] ([Col1] [varchar] (200))";

        assertEquals("Not a valid destination name.", dbs.checkQuery(sqlQuery));
    }

    @Test
    void testInsertDataValidInsertion() {
        LinkedHashMap<String[], BigDecimal> testMap = new LinkedHashMap<>();
        List<Map<String[], BigDecimal>> testList = new LinkedList<>();

        String[] columnValues = {"050394", "2020"};
        testMap.put(columnValues, BigDecimal.valueOf(15000));

        testList.add(testMap);

        dbs.checkQuery("drop table " + validDest);

        String sqlQuery = "create table " + validDest + " ([Birthdate] [varchar] (6), [Tid] [int], [Money] [numeric] (10,2))";
        assertEquals("OK", dbs.checkQuery(sqlQuery));

        assertEquals("OK", dbs.insertData(testList, validDest));
    }

    @Test
    void testInsertDataBigValidInsertion() {
        LinkedHashMap<String[], BigDecimal> testMap = new LinkedHashMap<>();
        List<Map<String[], BigDecimal>> testList = new LinkedList<>();

        String[] columnValues = {"050394", "2020"};
        testMap.put(columnValues, BigDecimal.valueOf(15000));

        testList = Collections.nCopies(1000001, testMap);

        dbs.checkQuery("drop table " + validPersistingDest);

        String sqlQuery = "create table " + validPersistingDest + " ([Birthdate] [varchar] (6), [Tid] [int], [Money] [numeric] (10,2))";
        assertEquals("OK", dbs.checkQuery(sqlQuery));

        assertEquals("OK", dbs.insertData(testList, validPersistingDest));
    }

    @Test
    void testInsertDataInvalidTableName() {
        LinkedHashMap<String[], BigDecimal> testMap = new LinkedHashMap<>();
        List<Map<String[], BigDecimal>> testList = new LinkedList<>();

        String[] columnValues = {"050394", "2020"};
        testMap.put(columnValues, BigDecimal.valueOf(15000));

        testList.add(testMap);

        assertEquals("Not a valid table name.", dbs.insertData(testList, "[derpderp].[SSB_01234]"));
    }

}