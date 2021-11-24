package no.ks.fiks.database.service.api.v1.date.service;

import org.joda.time.DateTimeZone;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DateServiceConfig {

    @Bean
    DateService dateService() {
        return new JodaDateService(defaultTimeZone());
    }

    @Bean
    DateTimeZone defaultTimeZone() {
        return DateTimeZone.UTC;
    }
}
