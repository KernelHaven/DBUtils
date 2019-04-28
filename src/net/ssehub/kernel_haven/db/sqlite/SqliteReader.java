/*
 * Copyright 2018-2019 University of Hildesheim, Software Systems Engineering
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.ssehub.kernel_haven.db.sqlite;

import static net.ssehub.kernel_haven.db.AbstractSqlTableCollection.escapeSqlIdentifier;
import static net.ssehub.kernel_haven.util.null_checks.NullHelpers.notNull;

import java.io.IOException;
import java.sql.Connection;
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
    
    private @NonNull String escapedTableName;
    
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
        this.escapedTableName = escapeSqlIdentifier(tableName);
        
        init();
    }
    
    /**
     * Initializes the reading. Loads column information and fires the query (sets {@link #resultSet} and
     * {@link #nColumns}).
     * 
     * @throws IOException If setting up or executing the SQL query fails.
     */
    private void init() throws IOException {
        List<@NonNull String> columns = new ArrayList<>();
        try {
            // the sqlite-jdbc implementation of con.getMetaData().getColumns() is bugged, since it's not possible
            // to escape % and _
            // thus we execute an SQLite-specific PRAGMA to get the column names
            
            String columnNameSql = String.format("PRAGMA table_info(%s);",
                    escapedTableName);
            
            ResultSet resultSet = con.prepareStatement(columnNameSql).executeQuery();
            while (resultSet.next()) {
                String name = notNull(resultSet.getString("name"));
                columns.add(name);
            }
        } catch (SQLException exc) {
            throw new IOException("Couldn't determine columns for: " + getTableName(), exc);
        }
        
        if (columns.isEmpty()) {
            throw new IOException(getTableName() + " has no columns or doesn't exist");
        }
        
        header = new  @NonNull String[columns.size()];
        int i = 0;
        StringBuilder columnNamesSql = new StringBuilder();
        for (String column : columns) {
            if (i != 0) {
                columnNamesSql.append(", ");
            }
            columnNamesSql.append(escapeSqlIdentifier(column));
            
            header[i++] = column;
        }
        
        String sql = String.format("SELECT %s FROM %s ORDER BY \"_rowid_\";",
                columnNamesSql.toString(),
                escapedTableName
        );
        
        try {
            resultSet = notNull(con.prepareStatement(sql).executeQuery());
            nColumns = columns.size();
            
        } catch (SQLException e) {
            throw new IOException("Couldn't execute SQL query for " + getTableName() + ": " + sql, e);
        }
    }
    
    @Override
    public void close() throws IOException {
        try {
            con.close();
        } catch (SQLException exc) {
            throw new IOException("Could not close connection for " + getTableName(), exc);
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
