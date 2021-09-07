package no.ks.fiks.database.service.api.v1.service;

import no.ks.fiks.database.service.api.v1.config.SqlConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.Arrays;

@Component
public class DatabaseService {

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

        createColumnRegex = "\\((?:\\[\\w+\\] (" + varcharRegex + "|" + intRegex + "|" + numericRegex + ")(\\, )?)+\\)";
        createColumnRegex2 = "(?:\\[\\w+\\] (" + varcharRegex + "|" + intRegex + "|" + numericRegex + ")(\\, )?)+";

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
        int test;

        if (sqlStatement.matches(createQueryRegex)) {
            querySplit = sqlStatement.replaceAll(createCommandRegex + " ", "").split(" ");

            if (!checkValidTableName(querySplit[0])) {
                return "Not a valid destination name.";
            }
            test = sqlStatement.replaceAll(createCommandRegex + " " + tableNameRegex, "").indexOf(createColumnRegex2);

            return ""+test;

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
