package no.ks.fiks.database.service.api.v1.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ks.fiks.Service.InsertTableService;
import no.ks.fiks.database.service.api.v1.config.SqlConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class DatabaseService {
    private final String createColumnRegexWithoutParenthesis;
    private final String createCommandRegex, dropTruncateCommandRegex, dropTruncateQueryRegex, createQueryRegex,
            tableNameRegex;//, schemaName;
    private final List<String> schemaNames;

    private final SqlConfiguration sqlConfig;
    private final JdbcTemplate jdbc;

    /**
     * Sets the different regexes to be used when checking for valid syntax on the different supported sql options
     *
     * @param jdbcTemplate The jdbcTemplate class with the correct connections
     */
    @Autowired
    public DatabaseService(JdbcTemplate jdbcTemplate) {
        final String createColumnRegexWithParenthesis;
        final String varcharRegex, intRegex, numericRegex;

        this.sqlConfig = new SqlConfiguration();
        this.jdbc = jdbcTemplate;

        //schemaName = sqlConfig.getSchemaName();
        schemaNames = sqlConfig.getSchemaNames();

        tableNameRegex = "((\\[\\w+\\]|(\\w+))\\.(\\[\\w+\\]|\\w+))";
        varcharRegex = "\\[varchar\\] \\(\\d+\\)";
        intRegex = "\\[int\\]";
        numericRegex = "\\[numeric\\] \\(\\d+\\,\\d+\\)";

        createColumnRegexWithParenthesis = "\\((?:\\[(\\w|\\s|æ|ø|å)+\\] (" + varcharRegex + "|" + intRegex + "|" + numericRegex + ")(\\, )?)+\\)";
        createColumnRegexWithoutParenthesis = "(?:(\\[\\w+\\]) (" + varcharRegex + "|" + intRegex + "|" + numericRegex + ")(\\, )?)+";

        dropTruncateCommandRegex = "((drop|truncate) table)";
        dropTruncateQueryRegex = dropTruncateCommandRegex + " " + tableNameRegex;

        createCommandRegex = "(create table)";
        createQueryRegex = createCommandRegex + " " + tableNameRegex + " " + createColumnRegexWithParenthesis;
    }

    /**
     * Checks if the input string is equal to any of the strings in the array
     *
     * @param inputStr the input string to be checked, e.g. a column name
     * @param items    an array containing the words to compare with the input string
     * @return true if the input string matches any of the items in the array, else false
     */
    private static boolean stringContainsItemFromList(String inputStr, String[] items) {
        return Arrays.stream(items).anyMatch(inputStr::equalsIgnoreCase);
    }

    /**
     * Checks if an SQL query is valid
     * <p>
     * Checks if an SQL query is either a create, drop or truncate query.
     * If it's either a drop or truncate query, checks that the query matches the expected structure before running it.
     * If it's a create query it also checks the column declarations before running it.
     *
     * @param sqlQuery the SQL query that is supposed to be run
     * @return an error message if the table name isn't valid, the query doesn't match the required structure or an
     * SQL error code and corresponding message if the query fails
     * @see #checkAndRunCreateQuery(String)
     * @see #checkAndRunDropTruncateQuery(String)
     */
    public String checkQuery(String sqlQuery) {
        if (sqlQuery.matches(createQueryRegex)) {
            return checkAndRunCreateQuery(sqlQuery);
        } else if (sqlQuery.matches(dropTruncateQueryRegex)) {
            return checkAndRunDropTruncateQuery(sqlQuery);
        }

        return "[ERROR] Not a valid structure on query \"" + sqlQuery + "\".";
    }

    /**
     * Checks if the sql query is valid before running it
     *
     * @param sqlQuery the query to be checked and run
     * @return "Not a valid destionation name" if the query is invalid. An
     * @see #runSqlStatement(String)
     */
    private String checkAndRunDropTruncateQuery(String sqlQuery) {
        String[] querySplit;

        //Remove the drop/truncate table part from the string and split on whitespace
        querySplit = sqlQuery.replaceAll(dropTruncateCommandRegex + " ", "").split(" ");

        if (!checkValidTableName(querySplit[0]))
            return "Not a valid destination name.";

        return runSqlStatement(sqlQuery);
    }

    /**
     * Checks and run a create sql query.
     *
     * @param sqlQuery the create query to be run
     * @return depending on the result different messages. If everything went well "OK".
     * @see #checkValidTableName(String)
     * @see #checkValidColumnDeclaration(String)
     */
    private String checkAndRunCreateQuery(String sqlQuery) {
        Pattern regexPattern;
        Matcher matcher;
        String[] querySplit;

        querySplit = sqlQuery.replaceAll(createCommandRegex + " ", "").split(" ");
        regexPattern = Pattern.compile(createColumnRegexWithoutParenthesis);
        matcher = regexPattern.matcher(sqlQuery.replaceAll(createCommandRegex + " " + tableNameRegex, ""));

        if (!checkValidTableName(querySplit[0])) {
            return "Not a valid destination name.";
        }

        if (matcher.find()) {
            if (checkValidColumnDeclaration(matcher.group(0))) {
                return runSqlStatement(sqlQuery);
            } else {
                return "Not a valid column declaration.";
            }
        }

        return "The input did not match the expected format.";
    }

    /**
     * Checks if a table name is valid.
     * Checks if the destination name contains one dot only, if the schema is a valid one or if it uses any reserved
     * words.
     *
     * @param dest The destination name of the table on the form [schema].[tableName]
     * @return false if it doesn't match the standard else true
     */
    public boolean checkValidTableName(String dest) {
        final String destSchemaName, tableName;
        final String[] destSplit;

        destSplit = dest.split("\\.");

        destSchemaName = destSplit[0];
        tableName = destSplit[1];

        //Always use the same schema
        /*if (!destSchemaName.equals(schemaName)) {
            System.out.println("Invalid schema name. Not a supported schema.");
            return false;
        }*/
        if (!schemaNames.contains(destSchemaName)) {
            System.out.println("Invalid schema name. Not a supported schema.");
            return false;
        }

        //Make sure that no protected words are used
        if (stringContainsItemFromList(tableName, sqlConfig.getKeywordList())) {
            System.out.println("Invalid table name. Reserved word.");
            return false;
        }

        return true;
    }

    /**
     * Checks if the given column declaration is valid.
     * Checks if the column name uses a reserved word and if the size of the type is inside the allowed range.
     *
     * @param columnDeclaration the column declaration(s)
     * @return false if any of the declarations are invalid
     * @see #checkColumnSizeValues(String, String)
     */
    private boolean checkValidColumnDeclaration(String columnDeclaration) {
        String[] columnDeclarationSplit = columnDeclaration.split(", ");

        for (String columnDecl : columnDeclarationSplit) {
            String[] columnDeclValues = columnDecl.replaceAll("[\\[\\]()]", "").split(" ");

            if (columnDeclValues.length < 2) {
                System.out.println("Should be at least two parameters for each column.");
                return false;
            }

            if (stringContainsItemFromList(columnDeclValues[0],
                    sqlConfig.getKeywordList())) {
                System.out.println("[ERROR] " + columnDeclValues[0] + " is an invalid column name.");
                return false;
            }

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

    /**
     * Checks if the given size in a column declaration is inside the valid range.
     * Supported types are varchar and numeric. Gets the size range from the SqlConfiguration class.
     *
     * @param type   The type of the column, e.g. varchar or numeric
     * @param values The size of the column. Numeric are separated by ",".
     * @return false if the column declaration isn't valid
     * @see SqlConfiguration
     */
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
            default -> {
                System.out.println(type + " is a non-supported column type.");
                return false;
            }
        }

        return true;
    }

    /**
     * Runs the SQL query
     * Tries to run the query provided and returns an error if an exception occurs
     *
     * @param query the query provided
     * @return an error message on the form "SQL Error: [errorCode]. [errorMessage]" if something went wrong, else "OK"
     */
    private String runSqlStatement(String query) {
        try {
            jdbc.execute(query);
        } catch (DataAccessException e) {
            SQLException se = (SQLException) e.getRootCause();

            try {
                assert se != null;
                return "SQL Error: " + se.getErrorCode() + ". " + se.getMessage();
            } catch (NullPointerException npe) {
                return "Something went wrong while getting the SQL error code";
            }
        } catch (Exception e) {
            System.out.println("Prob here?");
            System.out.println(query);
            System.out.println(jdbc.getDataSource());
            return e.getClass().getName();
        }
        return "OK";
    }

    public String convertJsonToInsertQuery(String json) {
        InsertTableService its = new InsertTableService();
        ObjectMapper om = new ObjectMapper();

        try {
            JsonNode jn = om.readTree(json);
            String tableName = jn.get("tableName").asText();

            if (tableName == null)
                return "[Error] Missing field tableName.";

            if (jn.get("data") == null)
                return "[Error] Missing field data.";

            List<String> data = Arrays.asList(om.writeValueAsString(jn.get("data")));

            return insertData(its.structureJsonStatTable(data), tableName);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "[ERROR] JsonProcessingException when converting insert json.";
        } catch (IOException ie) {
            ie.printStackTrace();
            return "[ERROR] IOException when converting insert json.";
        }
    }

    /**
     * Inserts the data from a list into a table in the db.
     *
     * @param ssbResult the list containing the data fetched from SSB
     * @param tableName the name of the table to insert data into in the db
     * @return the result of the insert or an error message
     * @see #batchUpdateData(List, String)
     */
    public String insertData(List<Map<String[], BigDecimal>> ssbResult, String tableName) {
        int maxNumberOfRows = 1000000;
        String result = "";

        if (!checkValidTableName(tableName))
            return "Not a valid table name.";

        if (ssbResult.size() > maxNumberOfRows) {
            int index;
            for (index = 0; index < ssbResult.size() / maxNumberOfRows; index++) {
                List<Map<String[], BigDecimal>> sublist = ssbResult.subList(index * maxNumberOfRows, (index + 1) * maxNumberOfRows);

                result = batchUpdateData(sublist, tableName);

                if (!result.equals("OK"))
                    return result;
            }

            if (ssbResult.size() % maxNumberOfRows > 0) {
                List<Map<String[], BigDecimal>> sublist = ssbResult.subList(index * maxNumberOfRows, ssbResult.size());
                result = batchUpdateData(sublist, tableName);
            }

            return result;
        } else {
            return batchUpdateData(ssbResult, tableName);
        }
    }

    /**
     * Inserts data from the list into the table with corresponding table name in the database
     *
     * @param ssbResult the list containing the data fetched from SSB
     * @param tableName the name of the table to insert data into
     * @return "OK"
     */
    private String batchUpdateData(List<Map<String[], BigDecimal>> ssbResult,
                                   String tableName) {
        StopWatch timer = new StopWatch();
        String valuesParam = "";

        for (String[] sa : ssbResult.get(0).keySet()) {
            for (int i = 0; i < sa.length; i++) {
                valuesParam += "?,";
            }
        }
        //TODO
        valuesParam += "?";

        System.out.println("batchUpdate -> Starting");

        timer.start();
        jdbc.batchUpdate(
                "insert into " + tableName + " values (" + valuesParam + ")",
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        for (String[] sa : ssbResult.get(i).keySet()) {
                            int c;

                            for (c = 0; c < sa.length; c++) {
                                preparedStatement.setString(c + 1, sa[c]);
                            }

                            preparedStatement.setBigDecimal(c + 1, ssbResult.get(i).get(sa));
                        }
                    }

                    @Override
                    public int getBatchSize() {
                        return ssbResult.size();
                    }
                }
        );
        timer.stop();

        System.out.println("batchUpdate -> Total time in seconds: " + timer.getTotalTimeSeconds());

        return "OK";
    }
}
