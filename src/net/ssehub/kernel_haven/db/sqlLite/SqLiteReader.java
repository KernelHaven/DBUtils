package net.ssehub.kernel_haven.db.sqlLite;

import static net.ssehub.kernel_haven.db.AbstractSqlTableCollection.sqlifyIdentifier;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.ssehub.kernel_haven.util.io.ITableReader;
import net.ssehub.kernel_haven.util.null_checks.NonNull;
import net.ssehub.kernel_haven.util.null_checks.Nullable;

/**
 * Reader for a table from a SQLite database.
 * 
 * @author El-Sharkawy
 */
public class SqLiteReader implements ITableReader {
    
    private @NonNull Connection con;
    private @NonNull String dbName;
    private @NonNull String tableName;
    private PreparedStatement sqlSelectStatement;
    private int nColumns;
    private String[][] content;
    private int rowIndex;

    /**
     * Creates a reader for the given table.
     * 
     * @param con A connection, exclusively used by this reader.
     * @param dbName The name of the database (used in log messages only).
     * @param tableName The name of the table to be ready from the database.
     * 
     * @throws IOException If reading the given table fails.
     */
    SqLiteReader(@NonNull Connection con, @NonNull String dbName, @NonNull String tableName) throws IOException {
        this.con = con;
        this.dbName = dbName;
        this.tableName = tableName;
        determineRelevantColumns();
        loadData();
    }
    
    /**
     * Selects all columns except for an optional ID column. However, if an ID column is present, the data is sorted
     * by the ID.
     * 
     * @throws IOException If reading the columns fails.
     */
    private void determineRelevantColumns() throws IOException {
        List<String> columns = new ArrayList<>();
        boolean hasID = false;
        try {
            DatabaseMetaData metadata = con.getMetaData();
            ResultSet resultSet = metadata.getColumns(null, null, tableName, null);
            while (resultSet.next()) {
                String name = resultSet.getString("COLUMN_NAME");
                String type = resultSet.getString("TYPE_NAME");
    
                if (!"ID".equals(name) && !"INTEGER".equals(type)) {
                    columns.add(sqlifyIdentifier(name));
                } else {
                    hasID = true;
                }
            }
        } catch (SQLException exc) {
            throw new IOException("Could determine columns for: " + getTableName(), exc);
        }
        
        if (columns.isEmpty()) {
            throw new IOException(getTableName() + " has no columns");
        }
        
        StringBuffer sql = new StringBuffer("SELECT ");
        sql.append(columns.get(0));
        for (int i = 1; i < columns.size(); i++) {
            sql.append(", ");
            sql.append(columns.get(i));
        }
        sql.append(" FROM ");
        sql.append(sqlifyIdentifier(tableName));
        
        if (hasID) {
            sql.append(" ORDER BY ID");
        }
        
        try {
            String sqlSelectQuery = sql.toString();
            sqlSelectStatement = con.prepareStatement(sqlSelectQuery);
            nColumns = columns.size();
        } catch (SQLException e) {
            throw new IOException("Couldn't create sql statement \"" + sql.toString() + "\" for: " + getTableName(),
                    e);
        }
    }
    
    @Override
    public void close() throws IOException {
        content = null;
        try {
            con.close();
        } catch (SQLException exc) {
            throw new IOException("Could not close connection for: " + getTableName(), exc);
        }
    }

    @Override
    public @NonNull String @Nullable [] readNextRow() throws IOException {
        return rowIndex < content.length ? content[rowIndex++] : null;
    }

    /**
     * Loads all data to provide line based reading as well as reading the complete data in one step.
     * 
     * @throws IOException If reading the data fails.
     */
    private void loadData() throws IOException {
        try {
            List<String[]> rows = new ArrayList<>();
            ResultSet rs = sqlSelectStatement.executeQuery();
            while (rs.next()) {
                String[] row = new String[nColumns];
                for (int i = 0; i < row.length; i++) {
                    row[i] = rs.getString((i + 1));
                }
                rows.add(row);
            }
            content = rows.toArray(new String[0][]);
        } catch (SQLException e) {
            throw new IOException("Could not execute query \"" + sqlSelectStatement.toString()
                + "\" for: " + getTableName(), e);
        }
    }

    @Override
    public int getLineNumber() {
        return rowIndex;
    }
    
    /**
     * Returns the name of the database and the table for logging information.
     * 
     * @return The name of the database and the table for logging information.
     */
    private String getTableName() {
        return tableName + " in " + dbName;
    }

}
