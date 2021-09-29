package no.ks.fiks.database.service.api.v1.controller;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseServiceControllerTest {

    private DatabaseServiceController dbsc;

    @BeforeEach
    void setUp() {
        dbsc = new DatabaseServiceController();
    }

    @AfterEach
    void tearDown() {

    }

    @Test
    void test1() {
    }

    @Test
    void createValidTable() {
        assertEquals("OK", dbsc.createTable("11933"));
    }

    @Test
    void dropTable() {
    }

    @Test
    void trunacteTable() {
    }
}