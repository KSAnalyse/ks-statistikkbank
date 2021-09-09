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
    private Pattern createColumnRegex3;
    private Matcher matcher;
    private final String createCommandRegex, dropCommandRegex, dropQueryRegex, createQueryRegex,
            truncateCommandRegex, truncateQueryRegex, tableNameRegex, schemaName, createDestParamSplitRegex,
            createColumnRegex;
    private final String createColumnRegex2;

    private String varcharRegex, intRegex, numericRegex;

    private final SqlConfiguration sqlConfig;
    private final String columnNameRegex;

    @Autowired
    public DatabaseService(SqlConfiguration sqlConfig) {
        this.sqlConfig = sqlConfig;

        schemaName = sqlConfig.getSchemaName();

        //[schemaName]|schemaName.[tableName]|tableName
        tableNameRegex = "((\\[\\w+\\]|(\\w+))\\.(\\[\\w+\\]|\\w+))";
        columnNameRegex = "\\[([a-z]|[A-Z]|[0-9]|_)+\\]";

        varcharRegex = "\\[varchar\\] \\(\\d+\\)";
        intRegex = "\\[int\\]";
        numericRegex = "\\[numeric\\] \\(\\d+\\,\\d+\\)";

        createColumnRegex = "\\((?:\\[(\\w|\\s)+\\] (" + varcharRegex + "|" + intRegex + "|" + numericRegex + ")(\\, )?)+\\)";
        createColumnRegex2 = "(?:(\\[\\w+\\]) (" + varcharRegex + "|" + intRegex + "|" + numericRegex + ")(\\, )?)+";

        dropCommandRegex = "(drop table)";
        dropQueryRegex = dropCommandRegex + " " + tableNameRegex;

        createCommandRegex = "(create table)";
        //split on whitespace that are not preceded by a-z, A-Z, 0-9 or ','
        createDestParamSplitRegex = "((?<![a-z]|[A-Z]|[0-9]|\\,) )";
        createQueryRegex = createCommandRegex + " " + tableNameRegex + " " + createColumnRegex;

        truncateCommandRegex = "(truncate table)";
        truncateQueryRegex = truncateCommandRegex + " " + tableNameRegex;
    }

    public String checkSqlStatement(JdbcTemplate jdbcTemplate, String sqlStatement) {
        String[] querySplit;

        if (sqlStatement.matches(createQueryRegex)) {
            querySplit = sqlStatement.replaceAll(createCommandRegex + " ", "").split(" ");
            createColumnRegex3 = Pattern.compile(createColumnRegex2);
            matcher = createColumnRegex3.matcher(sqlStatement.replaceAll(createCommandRegex + " " + tableNameRegex, ""));
            if (!checkValidTableName(querySplit[0])) {
                return "Not a valid destination name.";
            }

            if (matcher.find()) {
                if (checkValidColumnDeclaration(matcher.group(0))) {
                    System.out.println(sqlStatement);
                    return runSqlStatement(jdbcTemplate, sqlStatement);
                } else {
                    return "not cool";
                }
            }

            //test = sqlStatement.replaceAll(createCommandRegex + " " + tableNameRegex, "").indexOf(createColumnRegex2);

            return "Doh!";

        } else if (sqlStatement.matches(dropQueryRegex)) {
            querySplit = sqlStatement.replaceAll(dropCommandRegex + " ", "").split(" ");

            if (!checkValidTableName(querySplit[0]))
                return "Not a valid destination name.";

            return runSqlStatement(jdbcTemplate, sqlStatement);

        } else if (sqlStatement.matches(truncateQueryRegex)) {
            querySplit = sqlStatement.replaceAll(truncateCommandRegex + " ", "").split(" ");

            if (!checkValidTableName(querySplit[0]))
                return "Not a valid destination name.";

            return runSqlStatement(jdbcTemplate, sqlStatement);
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
            case "varchar":
                if (values.isBlank()) {
                    System.out.println("Varchar must have a defined size on the form (size)");
                    return false;
                }

                int varcharSize = Integer.parseInt(values);

                if (varcharSize > sqlConfig.getVarcharMaxSize()) {
                    System.out.println("Varchar size " + varcharSize + "is larger than the allowed max of " + sqlConfig.getVarcharMaxSize());
                    return false;
                }

                break;

            case "numeric":

                if (values.isBlank())
                    return false;

                String[] numericValues = values.split(",");

                if (numericValues.length != 2)
                    return false;

                if (Integer.parseInt(numericValues[0]) > sqlConfig.getNumericMaxPrecision())
                    return false;

                if (Integer.parseInt(numericValues[1]) > sqlConfig.getNumericMaxScale())
                    return false;

                break;

            case "default":
                System.out.println(type + " is a non-supported column type.");
                return false;
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
     * @param jdbcTemplate
     * @param query        the query provided
     * @return an error message if something went wrong, else "OK"
     */
    public String runSqlStatement(JdbcTemplate jdbcTemplate, String query) {
        try {
            System.out.println(query);
            jdbcTemplate.execute(query);
        } catch (DataAccessException e) {
            SQLException se = (SQLException) e.getRootCause();

            return "SQL Error: " + se.getErrorCode() + ". " + se.getMessage();
        } catch (Exception e) {
            return e.getClass().getName();
        }
        return "OK";
    }
}
