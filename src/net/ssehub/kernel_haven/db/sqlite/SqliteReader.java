package net.ssehub.kernel_haven.db.sqlite;

import static net.ssehub.kernel_haven.db.AbstractSqlTableCollection.escapeSqlIdentifier;
import static net.ssehub.kernel_haven.db.sqlite.SqliteCollection.ID_FIELD;
import static net.ssehub.kernel_haven.db.sqlite.SqliteCollection.ID_FIELD_ESCAPED;
import static net.ssehub.kernel_haven.util.null_checks.NullHelpers.notNull;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
 * @author Adam
 * @author El-Sharkawy
 */
public class SqliteReader implements ITableReader {
    
    private @NonNull Connection con;
    
    private @NonNull String dbName;
    
    private @NonNull String tableName;
    
    private @NonNull ResultSet resultSet;
    
    private @NonNull String @NonNull [] header;

    /**
     * Whether {@link #readNextRow()} already returned the header.
     */
    private boolean returnedHeader;
    
    private int nColumns;
    
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
    @SuppressWarnings("null") // resultSet and header are initialized in init()
    SqliteReader(@NonNull Connection con, @NonNull String dbName, @NonNull String tableName) throws IOException {
        this.con = con;
        this.dbName = dbName;
        this.tableName = tableName;
        
        init();
    }
    
    /**
     * <p>
     * Initializes the reading. Loads column information and fires the query (sets {@link #resultSet} and
     * {@link #nColumns}).
     * </p>
     * <p>
     * Selects all columns except for an optional ID column. However, if an ID column is present, the data is sorted
     * by the ID. See {@link SqliteCollection#ID_FIELD}.
     * </p>
     * 
     * @throws IOException If setting up or executing the SQL query fails.
     */
    private void init() throws IOException {
        List<String> columns = new ArrayList<>();
        boolean hasID = false;
        try {
            DatabaseMetaData metadata = con.getMetaData();
            ResultSet resultSet = metadata.getColumns(null, null, tableName, null);
            while (resultSet.next()) {
                String name = resultSet.getString("COLUMN_NAME");
    
                if (!ID_FIELD.equals(name)) {
                    columns.add(name);
                } else {
                    hasID = true;
                }
            }
        } catch (SQLException exc) {
            throw new IOException("Could determine columns for: " + getTableName(), exc);
        }
        
        if (columns.isEmpty()) {
            throw new IOException(getTableName() + " has no columns or doesn't exist");
        }
        
        header = new  @NonNull String[columns.size()];
        
        StringBuffer sql = new StringBuffer()
                .append("SELECT ")
                .append(escapeSqlIdentifier(notNull(columns.get(0))));
        header[0] = notNull(columns.get(0));
        for (int i = 1; i < columns.size(); i++) {
            sql
                .append(", ")
                .append(escapeSqlIdentifier(notNull(columns.get(i))));
            header[i] = notNull(columns.get(i));
        }
        sql.append(" FROM ")
            .append(escapeSqlIdentifier(tableName));
        
        if (hasID) {
            sql.append(" ORDER BY " + ID_FIELD_ESCAPED);
        }
        
        String sqlSelectQuery = sql.toString();
        try {
            resultSet = notNull(con.prepareStatement(sqlSelectQuery).executeQuery());
            nColumns = columns.size();
            
        } catch (SQLException e) {
            throw new IOException("Couldn't execute SQL statement \"" + sqlSelectQuery + "\" for: " + getTableName(),
                    e);
        }
    }
    
    @Override
    public void close() throws IOException {
        try {
            con.close();
        } catch (SQLException exc) {
            throw new IOException("Could not close connection for: " + getTableName(), exc);
        }
    }

    @Override
    public @NonNull String @Nullable [] readNextRow() throws IOException {
        @NonNull String[] result = null;
        
        if (!returnedHeader) {
            // first iteration will return the header
            result = header;
            returnedHeader = true;
            
        } else {
            try {
                
                if (resultSet.next()) {
                    rowIndex++;
                    result = new @NonNull String[nColumns];
                    
                    for (int i = 0; i < result.length; i++) {
                        String value = resultSet.getString(i + 1);
                        if (value == null) {
                            value = "";
                        }
                        result[i] = value;
                    }
                }
                
            } catch (SQLException e) {
                throw new IOException("Can't read next row in " + getTableName(), e);
            }
        }
        
        return result;
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