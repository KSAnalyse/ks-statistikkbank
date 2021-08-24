package no.ks.fiks.database.service.api.v1.service;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DatabaseService {

    private Pattern createPattern;


    //TODO: Sett inn flere sjekk som lengde på createString/dropString (hva er for stort?), evt andre sjekk.


    private Matcher findMatch(String tablePattern, String sqlString) {
        createPattern = Pattern.compile(tablePattern);
        return createPattern.matcher(sqlString);

    }
    //create table (Regionkode varchar(255), Statistikkvariabelkode varchar(255))
    private List<String> splitSqlStatment(String sqlStatement, String sqlRegex) {
        List<String> sql = Arrays.asList(sqlStatement.split(sqlRegex));
        sql = sql.stream().filter(item -> !item.isEmpty()).collect(Collectors.toList());
        return sql;
    }

    public String checkSqlStatement(JdbcTemplate jdbcTemplate, String sqlStatement) {
        List<String> splitCommand = null;
        if (sqlStatement.contains("create")) {
            final String createRegex = "((?<!\\,|create) (?!varchar|numeric|int|(not )?null))";
            splitCommand = splitSqlStatment(sqlStatement, createRegex);
            if(checkSplitSqlStatement(splitCommand, "create table", "TEST_S"))
                return runSqlStatement(jdbcTemplate, sqlStatement);
            else
                System.out.println("Something was wrong with your create statement");
        }
        else if (sqlStatement.contains("drop")) {
            final String dropRegex = "((?<!drop) )";
            splitCommand = splitSqlStatment(sqlStatement, dropRegex);

            if(checkSplitSqlStatement(splitCommand, "drop table", "TEST_S"))
                return runSqlStatement(jdbcTemplate, sqlStatement);
            else
                System.out.println("Something was wrong with your drop statement");
        }
        else if (sqlStatement.contains("truncate")) {
            final String truncateRegex = "((?<!truncate) )";
            splitCommand = splitSqlStatment(sqlStatement, truncateRegex);
            if(checkSplitSqlStatement(splitCommand, "truncate table", "TEST_S"))
                return runSqlStatement(jdbcTemplate, sqlStatement);
            else
                System.out.println("Something was wrong with your truncate statement");
        }
        /*else if (sqlStatement.contains("insert")) {
            splitCommand = splitSqlStatment(sqlStatement, insertRegex);
            System.out.println(checkInsertStatement(splitCommand));
        }
        else if (sqlStatement.contains("delete")) {
            splitCommand = splitSqlStatment(sqlStatement, deleteRegex);
            System.out.println(checkDeleteStatement(splitCommand));
        }
        else if (sqlStatement.contains("update")) {
            splitCommand = splitSqlStatment(sqlStatement, updateRegex);
            System.out.println(checkUpdateStatement(splitCommand));
        }
        return null;*/
        return sqlStatement;
    }

    private boolean checkSplitSqlStatement(List<String> splitSqlStatement, String command, String destinationSchema) {
        String sqlCommand = "";
        int statementLength = 0;
        System.out.printf("Create Statement has %d parts: %n", splitSqlStatement.size());
        switch (command) {
            case "create table":
                statementLength = 3;
                break;
            case "drop table":
            case "truncate table":
                statementLength = 2;
                break;
        }

        if (splitSqlStatement.size() == statementLength) {
            System.out.println(splitSqlStatement);
            if (splitSqlStatement.get(0).equals(command)) {
                sqlCommand += splitSqlStatement.get(0) + " ";
                if (splitSqlStatement.get(1).matches(destinationSchema + "(\\.)\\w+\\S+")) {
                    sqlCommand += splitSqlStatement.get(1) + " ";
                    if (splitSqlStatement.size() > 2) {
                        sqlCommand += splitSqlStatement.get(2);
                    }
                }
            }
        } else {
            System.out.printf("Your %s statement has too few/many arguments...", command);
            return false;
        }
        System.out.println(sqlCommand);
        return true;
    }

/*
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
*/

    /** Runs the SQL query
     *
     * Tries to run the query provided and returns an error if an exception occurs
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
