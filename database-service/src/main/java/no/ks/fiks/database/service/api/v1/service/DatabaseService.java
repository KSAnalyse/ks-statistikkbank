package no.ks.fiks.database.service.api.v1.service;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatabaseService {

    private Pattern createPattern;
    private Matcher createMatcher;
    private String tablePattern;

    //TODO: Sett inn flere sjekk som lengde på createString/dropString (hva er for stort?), evt andre sjekk.

    //TODO: Splitt opp table, create og matcher i en egen hjelpemetode som returnerer createMatcher.
    public String createTable(JdbcTemplate jdbcTemplate, String createString) {
        try {
            tablePattern = "(create table [A-Za-z_]+\\.\\w+) \\(\\w+ \\w+(\\(\\d+\\))?\\)";
            createPattern = Pattern.compile(tablePattern);
            createMatcher = createPattern.matcher(createString);

            if (createMatcher.find())
                jdbcTemplate.execute(createString);
            else
                return "Create table string didn't match the pattern, try with lower case";
        } catch (UncategorizedSQLException e) {
            return Objects.requireNonNull(e.getMessage()).split(":")[1].trim();
        }
        return "Table created";
    }

    public String dropTable(JdbcTemplate jdbcTemplate, String dropString) {
        try {
            tablePattern = "(drop table [A-Za-z_]+\\.\\w+)";
            createPattern = Pattern.compile(tablePattern);
            createMatcher = createPattern.matcher(dropString);

            if (createMatcher.find())
                jdbcTemplate.execute(dropString);
            else
                return "Drop table string didn't match the pattern, try with lower case";

        } catch (UncategorizedSQLException e) {
            return Objects.requireNonNull(e.getMessage()).split(":")[1].trim();
        }

        return "Table dropped";
    }
}
