package no.ks.fiks.database.service.api.v1.date.service;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.TimeZone;

public class JodaDateService implements DateService{

    private final DateTimeZone timeZone;

    JodaDateService(final DateTimeZone timeZone) {
        super();
        this.timeZone = timeZone;
        System.setProperty("user.timezone", timeZone.getID());
        TimeZone.setDefault(timeZone.toTimeZone());
        DateTimeZone.setDefault(timeZone);
    }

    @Override
    public DateTime now() {
        return DateTime.now(timeZone);
    }
}
