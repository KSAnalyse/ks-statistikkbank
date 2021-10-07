package no.ks.fiks.database.service.api.v1.controller;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DatabaseServiceControllerTest {

    private DatabaseServiceController dbsc;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        dbsc = new DatabaseServiceController(jdbc);
    }

    @AfterEach
    void tearDown() {

    }

    @Test
    void test1() {
    }

    @Test
    void createValidTableWithOnlyTableCode() {
        assertEquals("OK", dbsc.createTable(
                "{\"tableCode\":\"11814\"}"
        ));
    }

    @Test
    void createValidTableWithOnlyYears() {
        assertEquals("OK", dbsc.createTable(
                "{\"tableCode\":\"11805\",\"numberOfYears\":\"5\"}"
        ));
    }

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
    void dropTable() {
    }

    @Test
    void trunacteTable() {
    }
}