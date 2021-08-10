package no.ks.fiks.database.service.api.v1.service;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DatabaseService {

    private Pattern createPattern;
    private Matcher createMatcher;

    //TODO: Sett inn flere sjekk som lengde på createString/dropString (hva er for stort?), evt andre sjekk.


    private Matcher findMatch(String tablePattern, String sqlString) {
        createPattern = Pattern.compile(tablePattern);
        return createPattern.matcher(sqlString);

    }
    //create table (Regionkode varchar(255), Statistikkvariabelkode varchar(255))
    public List<String> splitSqlStatment(String sqlStatement) {
        List<String> sql = Arrays.asList(sqlStatement.split(""));
        sql = sql.stream().filter(item -> !item.isEmpty()).collect(Collectors.toList());
        sql.forEach(System.out::println);
        return sql;
    }

    private String checkSqlStatement(String sqlStatement) {
        List<String> splitSqlStatement = splitSqlStatment(sqlStatement);
        switch (splitSqlStatement.get(0).toLowerCase()) {
            case "create":
                checkCreateStatement(splitSqlStatement);
            case "drop":
                checkDropStatement(splitSqlStatement);
            case "truncate":
                checkTruncateStatement(splitSqlStatement);
            case "insert":
                checkInsertStatement(splitSqlStatement);
            case "delete":
                checkDeleteStatement(splitSqlStatement);
            case "update":
                checkUpdateStatement(splitSqlStatement);
        }
        return null;
    }

    private boolean checkCreateStatement(List<String> splitSqlStatement) {
        return false;
    }

    private boolean checkDropStatement(List<String> splitSqlStatement) {
        return false;
    }

    private boolean checkTruncateStatement(List<String> splitSqlStatement) {
        return false;
    }

    private boolean checkInsertStatement(List<String> splitSqlStatement) {
        return false;
    }

    private boolean checkDeleteStatement(List<String> splitSqlStatement) {
        return false;
    }

    private boolean checkUpdateStatement(List<String> splitSqlStatement) {
        return false;
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


    /** Runs the SQL statement
     *
     * Tries to run the statement provided and returns an error if an exception occurs
     *
     * @param jdbcTemplate
     * @param query the query provided
     * @return an error message if something went wrong, else "OK"
     */
    public String runSqlStatement(JdbcTemplate jdbcTemplate, String query) {
        int errorCode;

        try {
            jdbcTemplate.execute(query);
        } catch (DataAccessException e) {
            SQLException se = (SQLException) e.getRootCause();
            errorCode = se.getErrorCode();

            return "SqlException. Error: " + errorCode + ". " + se.getMessage();
        } catch (Exception e) {
            return e.getClass().getName();
        }
        return "OK";
    }
}
