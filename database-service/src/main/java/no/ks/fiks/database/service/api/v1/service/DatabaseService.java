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

    //TODO: Sett inn flere sjekk som lengde på createString/dropString (hva er for stort?), evt andre sjekk.


    private Matcher findMatch(String tablePattern, String sqlString) {
        createPattern = Pattern.compile(tablePattern);
        return createPattern.matcher(sqlString);

    }

    public String createTable(JdbcTemplate jdbcTemplate, String createString) {
        try {
            String tablePattern = "(create table [A-Za-z_]+\\.\\w+) \\(\\w+ \\w+(\\(\\d+\\))?\\)";

            if (findMatch(tablePattern, createString).find())
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
            String tablePattern = "(drop table [A-Za-z_]+\\.\\w+)";

            if (findMatch(tablePattern, dropString).find())
                jdbcTemplate.execute(dropString);
            else
                return "Drop table string didn't match the pattern, try with lower case";

        } catch (UncategorizedSQLException e) {
            return Objects.requireNonNull(e.getMessage()).split(":")[1].trim();
        }

        return "Table dropped";
    }
}
