package no.ks.fiks.database.service;

import no.ks.fiks.database.service.api.v1.service.DatabaseService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class App {
    public static void main( String[] args ) {
        SpringApplication.run(App.class, args);
        //new DatabaseService().checkQuery("create table TEST_S.Test (Regionkode varchar(255) null, Statistikkvariabelkode varchar(255) not null, Test int not null, Verdi numeric(18,0) null)");
    }
}
