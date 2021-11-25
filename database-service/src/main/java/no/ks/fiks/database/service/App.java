package no.ks.fiks.database.service;

import no.ks.fiks.database.service.api.v1.service.DatabaseService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class App {
    public static void main( String[] args ) {
        try {
            SpringApplication.run(App.class, args);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
