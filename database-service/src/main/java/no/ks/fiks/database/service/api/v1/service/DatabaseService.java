package no.ks.fiks.database.service.api.v1.service;

import no.ks.fiks.database.service.api.v1.config.SqlConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class DatabaseService {
    private final String createColumnRegexWithoutParenthesis;
    private final String createCommandRegex, dropCommandRegex, dropQueryRegex, createQueryRegex,
            truncateCommandRegex, truncateQueryRegex, tableNameRegex, schemaName;

    private final SqlConfiguration sqlConfig;
    private final String insertCommandRegex, insertQueryRegex;
    private final String valuesRegex;

    @Autowired
    public DatabaseService(SqlConfiguration sqlConfig) {
        final String createColumnRegexWithParenthesis;
        final String varcharRegex, intRegex, numericRegex;
        this.sqlConfig = sqlConfig;

        schemaName = sqlConfig.getSchemaName();

        tableNameRegex = "((\\[\\w+\\]|(\\w+))\\.(\\[\\w+\\]|\\w+))";

        varcharRegex = "\\[varchar\\] \\(\\d+\\)";
        intRegex = "\\[int\\]";
        numericRegex = "\\[numeric\\] \\(\\d+\\,\\d+\\)";

        valuesRegex = "\\((?:(\'\\w+\'|\\d+\\.?\\d*)(\\, )?)+\\)";

        createColumnRegexWithParenthesis = "\\((?:\\[(\\w|\\s)+\\] (" + varcharRegex + "|" + intRegex + "|" + numericRegex + ")(\\, )?)+\\)";
        createColumnRegexWithoutParenthesis = "(?:(\\[\\w+\\]) (" + varcharRegex + "|" + intRegex + "|" + numericRegex + ")(\\, )?)+";

        dropCommandRegex = "(drop table)";
        dropQueryRegex = dropCommandRegex + " " + tableNameRegex;

        createCommandRegex = "(create table)";
        createQueryRegex = createCommandRegex + " " + tableNameRegex + " " + createColumnRegexWithParenthesis;

        truncateCommandRegex = "(truncate table)";
        truncateQueryRegex = truncateCommandRegex + " " + tableNameRegex;

        insertCommandRegex = "(insert into)";
        insertQueryRegex = "(" + insertCommandRegex + " " + tableNameRegex + "( values )" + valuesRegex + ")";
    }

    /** Checks if an SQL query is valid
     *
     * Checks if an SQL query is either a create, drop or truncate query.
     * If it's either a drop or truncate query, checks that the query matches the expected structure before running it.
     * If it's a create query it also checks the column declarations before running it.
     *
     * @param jdbcTemplate the jdbcTemplate with the database configuration
     * @param sqlQuery the SQL query that is supposed to be run
     * @return an error message if the table name isn't valid, the query doesn't match the required structure or an
     *         SQL error code and corresponding message if the query fails
     */
    public String checkSqlStatement(JdbcTemplate jdbcTemplate, String sqlQuery) {
        Pattern regexPattern;
        Matcher matcher;
        String[] querySplit;

        if (sqlQuery.matches(createQueryRegex)) {
            querySplit = sqlQuery.replaceAll(createCommandRegex + " ", "").split(" ");
            regexPattern = Pattern.compile(createColumnRegexWithoutParenthesis);
            matcher = regexPattern.matcher(sqlQuery.replaceAll(createCommandRegex + " " + tableNameRegex, ""));

            if (!checkValidTableName(querySplit[0])) {
                return "Not a valid destination name.";
            }

            if (matcher.find()) {
                if (checkValidColumnDeclaration(matcher.group(0))) {
                    return runSqlStatement(jdbcTemplate, sqlQuery);
                } else {
                    return "Not a valid column declaration.";
                }
            }

            return "The input did not match the expected format.";

        } else if (sqlQuery.matches(dropQueryRegex)) {
            querySplit = sqlQuery.replaceAll(dropCommandRegex + " ", "").split(" ");

            if (!checkValidTableName(querySplit[0]))
                return "Not a valid destination name.";

            return runSqlStatement(jdbcTemplate, sqlQuery);

        } else if (sqlQuery.matches(truncateQueryRegex)) {
            querySplit = sqlQuery.replaceAll(truncateCommandRegex + " ", "").split(" ");

            if (!checkValidTableName(querySplit[0]))
                return "Not a valid destination name.";

            return runSqlStatement(jdbcTemplate, sqlQuery);
        } else if (sqlQuery.matches(insertQueryRegex)) {
            regexPattern = Pattern.compile(tableNameRegex);
            matcher = regexPattern.matcher(sqlQuery);

            if (matcher.find()) {
                if (!checkValidTableName(sqlQuery.substring(matcher.start(), matcher.end())))
                    return "Not a valid destination name.";

                return runSqlStatement(jdbcTemplate, sqlQuery);
            }

            return "fail";
        }

        return "Not a valid structure on query.";
    }

    public boolean checkValidTableName(String dest) {
        final String destSchemaName, tableName;
        final String[] destSplit;

        destSplit = dest.split("\\.");

        //Name should always be schema.tablename
        if (destSplit.length != 2) {
            System.out.println("Invalid destination name. Too many dots.");
            return false;
        }

        //Destination name should always follow the regex structure
        if (!checkValidTableNameStructure(dest)) {
            System.out.println("Invalid destination name. Doesn't match regex pattern.");
            return false;
        }

        destSchemaName = destSplit[0];
        tableName = destSplit[1];

        //Always use the same schema
        if (!destSchemaName.equals(schemaName)) {
            System.out.println("Invalid schema name. Not a supported schema.");
            return false;
        }

        //Make sure that no protected words are used
        if (stringContainsItemFromList(tableName, sqlConfig.getKeywordList())) {
            System.out.println("Invalid tablename. Reserved word.");
            return false;
        }

        return true;
    }

    private boolean checkValidColumnDeclaration(String columnDeclaration) {
        String[] columnDeclarationSplit = columnDeclaration.split(", ");

        for (String columnDecl : columnDeclarationSplit) {
            String[] columnDeclValues = columnDecl.replaceAll("[\\[\\]\\(\\)]", "").split(" ");

            if (columnDeclValues.length < 2) {
                System.out.println("Should be at least two parameters for each column.");
                return false;
            }

            if (stringContainsItemFromList(columnDeclValues[0],
                    sqlConfig.getKeywordList()))
                return false;

            if (columnDeclValues[1].equals("int")) {
                if (columnDeclValues.length != 2)
                    return false;
            } else {
                if (!checkColumnSizeValues(columnDeclValues[1], columnDeclValues[2]))
                    return false;
            }
        }

        return true;
    }

    private boolean checkColumnSizeValues(String type, String values) {

        switch (type) {
            case "varchar" -> {
                if (values.isBlank()) {
                    System.out.println("Varchar must have a defined size on the form (size)");
                    return false;
                }
                int varcharSize = Integer.parseInt(values);
                if (varcharSize > sqlConfig.getVarcharMaxSize()) {
                    System.out.println("Varchar size " + varcharSize + " is larger than the allowed max of " + sqlConfig.getVarcharMaxSize());
                    return false;
                }
            }
            case "numeric" -> {
                if (values.isBlank())
                    return false;
                String[] numericValues = values.split(",");
                if (numericValues.length != 2)
                    return false;
                if (Integer.parseInt(numericValues[0]) > sqlConfig.getNumericMaxPrecision())
                    return false;
                if (Integer.parseInt(numericValues[1]) > sqlConfig.getNumericMaxScale())
                    return false;
            }
            case "default" -> {
                System.out.println(type + " is a non-supported column type.");
                return false;
            }
        }

        return true;
    }

    private boolean checkValidTableNameStructure(String tableName) {
        return tableName.matches(tableNameRegex);
    }

    private static boolean stringContainsItemFromList(String inputStr, String[] items) {
        return Arrays.stream(items).anyMatch(inputStr::equalsIgnoreCase);
    }

    /**
     * Runs the SQL query
     * <p>
     * Tries to run the query provided and returns an error if an exception occurs
     *
     * @param jdbcTemplate jdbcTemplate used to connect to the database
     * @param query        the query provided
     * @return an error message if something went wrong, else "OK"
     */
    public String runSqlStatement(JdbcTemplate jdbcTemplate, String query) {
        try {
            jdbcTemplate.execute(query);
        } catch (DataAccessException e) {
            SQLException se = (SQLException) e.getRootCause();

            try {
                return "SQL Error: " + se.getErrorCode() + ". " + se.getMessage();
            } catch (NullPointerException npe) {
                return "Something went wrong while getting the SQL error code";
            }
        } catch (Exception e) {
            return e.getClass().getName();
        }
        return "OK";
    }
}
