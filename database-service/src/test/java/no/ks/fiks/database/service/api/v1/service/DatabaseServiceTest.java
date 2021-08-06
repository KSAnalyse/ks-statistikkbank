package no.ks.fiks.database.service.api.v1.service;

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
    private String tableName, resultString, inputString;

    @BeforeEach
    void setUp() {
        dbs = new DatabaseService();
        tableName = "TEST_S.TEST";
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
    void testCreateTableValidStringStructure() {
        inputString = "create table TEST_S.TEST (Regionkode varchar(100))";
        resultString = "Table created";

        assertEquals(resultString, dbs.createTable(jdbcTemplate, inputString));
    }

    @Test
    void testCreateTableInvalidStringStructure() {
        inputString = "create table (Regionkode varchar(100)) TEST_S.TEST";
        resultString = "Create table string didn't match the pattern, try with lower case";

        assertEquals(resultString, dbs.createTable(jdbcTemplate, inputString));
    }

    @Test
    void testDropTableValidStringStructure() {
        dbs.createTable(jdbcTemplate, "create table TEST_S.TEST (Regionkode int)");

        inputString = "drop table TEST_S.TEST";
        resultString = "Table dropped";

        assertEquals(resultString, dbs.dropTable(jdbcTemplate, inputString));
    }

    @Test
    void dropTable() {
    }
}