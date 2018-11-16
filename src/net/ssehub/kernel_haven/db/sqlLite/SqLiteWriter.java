package net.ssehub.kernel_haven.db.sqlLite;

import static net.ssehub.kernel_haven.db.AbstractSqlTableCollection.sqlifyIdentifier;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import net.ssehub.kernel_haven.util.io.AbstractTableWriter;
import net.ssehub.kernel_haven.util.io.TableRowMetadata;
import net.ssehub.kernel_haven.util.null_checks.NonNull;
import net.ssehub.kernel_haven.util.null_checks.Nullable;

/**
 * Writes to a table of a (local) SQLite database.
 * 
 * @author El-Sharkawy
 */
public class SqLiteWriter extends AbstractTableWriter {
    
    private @NonNull Connection con;
    private @NonNull String dbName;
    private @NonNull String tableName;
    
    // Normal writing
    private PreparedStatement sqlInsertStatement;
    private String sqlInsertQuery;
    
    // relation writing
    private PreparedStatement sqlInsertStatement1;
    private PreparedStatement sqlInsertStatement2;
    
    /**
     * Creates a writer for the given table.
     * 
     * @param con A connection, exclusively used by this writer.
     * @param dbName The name of the database (used in log messages only).
     * @param tableName The name of the table to be created inside the database.
     * 
     * @throws IOException If setting up the connection fails.
     */
    SqLiteWriter(@NonNull Connection con, @NonNull String dbName, @NonNull String tableName) throws IOException {
        this.con = con;
        this.dbName = dbName;
        this.tableName = tableName;
        
        try {
            // https://sqlite.org/pragma.html#pragma_foreign_keys
            con.prepareStatement("PRAGMA foreign_keys = 1;").execute();
            
            // https://sqlite.org/pragma.html#pragma_synchronous
            // setting this to OFF (= 0) increases write speed by more than 10 times
            con.prepareStatement("PRAGMA synchronous = 0;").execute();
            
        } catch (SQLException e) {
            throw new IOException("Could not set up connection for " + getTableName(), e);
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
    public void writeHeader(@Nullable Object /*@NonNull*/ ... fields) throws IOException {
        // Create the table containing the element definitions
        StringBuffer sqlCreate = new StringBuffer("CREATE TABLE ");
        sqlCreate.append(sqlifyIdentifier(tableName, null));
        sqlCreate.append(" (");
        sqlCreate.append("ID INTEGER PRIMARY KEY");
        
        StringBuffer sqlInsert = new StringBuffer("INSERT INTO ");
        sqlInsert.append(sqlifyIdentifier(tableName, null));
        sqlInsert.append(" VALUES (");
        sqlInsert.append("NULL");
        
        for (int i = 0; i < fields.length; i++) {
            sqlCreate.append(", ");
            String columnName = sqlifyIdentifier(tableName, fields[i].toString());
            sqlCreate.append(columnName);
            sqlCreate.append(" TEXT");
            
            sqlInsert.append(", ?");
        }
        
        sqlCreate.append(");");
        sqlInsert.append(");");
        try {
            PreparedStatement sqlStatement = con.prepareStatement(sqlCreate.toString());
            sqlStatement.execute();
            
            sqlInsertQuery = sqlInsert.toString();
            sqlInsertStatement = con.prepareStatement(sqlInsertQuery);
        } catch (SQLException exc) {
            throw new IOException("Could not prepare SQL queries", exc);
        }
    }
    
    @Override
    public void writeRow(@Nullable Object /*@NonNull*/ ... columns) throws IOException {
        if (sqlInsertStatement == null) {
            throw new IOException("writeHeader() must be called before writeRow()");
        }
        
        for (int i = 0; i < columns.length; i++) {
            try {
                sqlInsertStatement.setString((i + 1), columns[i].toString());
            } catch (SQLException e) {
                throw new IOException("Could not prepare statement " + i + " in " + sqlInsertQuery
                    + " with values: " + columns.toString() + " for: " + getTableName());
            }
        }
        
        try {
            sqlInsertStatement.execute();
        } catch (SQLException e) {
            throw new IOException("Could not execute query \"" + sqlInsertQuery + "\" for: " + getTableName());
        }
    }

    @Override
    public void flush() throws IOException {
        // No action required, all data is written directly into database.
    }
    
    /**
     * Returns the name of the database and the table for logging information.
     * 
     * @return The name of the database and the table for logging information.
     */
    private String getTableName() {
        return tableName + " in " + dbName;
    }
    
    @Override
    protected void writeAnnotationHeader(@NonNull TableRowMetadata metadata) throws IOException {
        if (!metadata.isRelation()) {
            // normal handling for non-relations
            super.writeAnnotationHeader(metadata);
            return;
        }
        
        // special handling for relations
        // create two tables: one ID content mapping, and one relation mapping (ID, ID)
        String elementTableName = sqlifyIdentifier(tableName + " Elements", null);
        String columnName = createElementsTable(elementTableName);
        
        @NonNull Object[] headers = metadata.getHeaders();
        if (headers.length < 2) {
            throw new IllegalArgumentException("Relation TableRows must have at least 2 TableElements");
        }
        createRelationTable(elementTableName, headers);
        
        // Create view combining the two tables
        createView(elementTableName, columnName, headers);
        
        StringBuffer sqlInsert1 = new StringBuffer();
        sqlInsert1.append("INSERT OR IGNORE INTO ");
        sqlInsert1.append(elementTableName);
        sqlInsert1.append(" VALUES (NULL, ?); ");
        
        StringBuffer sqlInsert2 = new StringBuffer();
        sqlInsert2.append("INSERT INTO ");
        sqlInsert2.append(sqlifyIdentifier(tableName, null));
        sqlInsert2.append(" VALUES (");
        
        sqlInsert2.append("(SELECT ID FROM ");
        sqlInsert2.append(elementTableName);
        sqlInsert2.append(" WHERE ");
        sqlInsert2.append(columnName);
        sqlInsert2.append(" = ?), ");
        
        sqlInsert2.append("(SELECT ID FROM ");
        sqlInsert2.append(elementTableName);
        sqlInsert2.append(" WHERE ");
        sqlInsert2.append(columnName);
        sqlInsert2.append(" = ?)");
        
        for (int i = 2; i < headers.length; i++) {
            sqlInsert2.append(", ?");
        }
        
        sqlInsert2.append(");");
        
        try {
            sqlInsertStatement1 = con.prepareStatement(sqlInsert1.toString());
            sqlInsertStatement2 = con.prepareStatement(sqlInsert2.toString());
        } catch (SQLException exc) {
            throw new IOException("Could not prepare insert statement", exc);
        }
    }

    /**
     * Part of {@link #writeAnnotationHeader(TableRowMetadata)}: Creates a view combining both tables.
     * @param elementTableName The name of the first table, which is used to reference elements as foreign key.
     * @param columnName The key column of the first table
     * @param headers A at least 2-dim array containing the names of the relationships.
     * @throws IOException If the view could not be created.
     */
    private void createView(String elementTableName, String columnName, Object[] headers) throws IOException {
        // Handling of optional, non-relational elements starting at 3rd index of header
        StringBuffer optionalColumns = null;
        if (headers.length > 2) {
            optionalColumns = new StringBuffer();
            for (int i = 2; i < headers.length; i++) {
                optionalColumns.append(", ");
                optionalColumns.append(sqlifyIdentifier(tableName, headers[i].toString()));
            }
        }
        
        
        // As we join two times the same table, we have to rename columns (and can't join in one step)
        // Inner select statement
        StringBuffer innerSelect = new StringBuffer("SELECT ");
        innerSelect.append(columnName);
        innerSelect.append(" AS ");
        innerSelect.append(sqlifyIdentifier(tableName, headers[0].toString()));
        innerSelect.append(", ");
        innerSelect.append(sqlifyIdentifier(tableName, headers[1].toString()));
        if (null != optionalColumns) {
            innerSelect.append(optionalColumns);
        }
        innerSelect.append(" FROM ");
        innerSelect.append(sqlifyIdentifier(tableName, null));
        innerSelect.append(" JOIN ");
        innerSelect.append(elementTableName);
        innerSelect.append(" ON ");
        innerSelect.append(sqlifyIdentifier(tableName, null));
        innerSelect.append(".");
        innerSelect.append(sqlifyIdentifier(tableName, headers[0].toString()));
        innerSelect.append(" = ");
        innerSelect.append(elementTableName);
        innerSelect.append(".ID");
        
        // Outer select statement
        StringBuffer outerSelect = new StringBuffer("SELECT ");
        outerSelect.append(sqlifyIdentifier(tableName, headers[0].toString()));
        outerSelect.append(", ");
        outerSelect.append(columnName);
        outerSelect.append(" AS ");
        outerSelect.append(sqlifyIdentifier(tableName, headers[1].toString()));
        if (null != optionalColumns) {
            outerSelect.append(optionalColumns);
        }
        outerSelect.append(" FROM (");
        outerSelect.append(innerSelect);
        outerSelect.append(") AS INNER_JOIN JOIN ");
        outerSelect.append(elementTableName);
        outerSelect.append(" ON INNER_JOIN");
        outerSelect.append(".");
        outerSelect.append(sqlifyIdentifier(tableName, headers[1].toString()));
        outerSelect.append(" = ");
        outerSelect.append(elementTableName);
        outerSelect.append(".ID");
        
        StringBuffer sqlCreateView = new StringBuffer("CREATE VIEW IF NOT EXISTS ");
        sqlCreateView.append(sqlifyIdentifier(tableName + " View", null));
        sqlCreateView.append(" AS ");
        sqlCreateView.append(outerSelect);
        try {
            PreparedStatement sqlStatement = con.prepareStatement(sqlCreateView.toString());
            sqlStatement.execute();
        } catch (SQLException exc) {
            throw new IOException("Could not create view", exc);
        }
    }

    /**
     * Part of {@link #writeAnnotationHeader(TableRowMetadata)}: Writes 2nd table, containing the relations between
     * elements of the first table (see {@link #createElementsTable(String)}).
     * @param elementTableName The name of the first table, which is used to reference elements as foreign key.
     * @param headers A at least 2-dim array containing the names of the relationships.
     * @throws IOException If the table could not be created.
     */
    private void createRelationTable(String elementTableName, Object[] headers) throws IOException {
        StringBuffer sqlCreate = new StringBuffer("CREATE TABLE ");
        sqlCreate.append(sqlifyIdentifier(tableName, null));
        sqlCreate.append(" (");
        
        sqlCreate.append(sqlifyIdentifier(tableName, headers[0].toString()));
        sqlCreate.append(" INTEGER, ");
        
        sqlCreate.append(sqlifyIdentifier(tableName, headers[1].toString()));
        sqlCreate.append(" INTEGER, ");
        
        for (int i = 2; i < headers.length; i++) {
            String columnName = sqlifyIdentifier(tableName, headers[i].toString());
            sqlCreate.append(columnName);
            sqlCreate.append(" TEXT, ");
        }
        
        sqlCreate.append(" FOREIGN KEY(");
        sqlCreate.append(sqlifyIdentifier(tableName, headers[0].toString()));
        sqlCreate.append(") REFERENCES ");
        sqlCreate.append(elementTableName);
        sqlCreate.append(" (ID),");
        
        sqlCreate.append(" FOREIGN KEY(");
        sqlCreate.append(sqlifyIdentifier(tableName, headers[1].toString()));
        sqlCreate.append(") REFERENCES ");
        sqlCreate.append(elementTableName);
        sqlCreate.append(" (ID)");
        
        sqlCreate.append(");");
        
        try {
            PreparedStatement sqlStatement = con.prepareStatement(sqlCreate.toString());
            sqlStatement.execute();
        } catch (SQLException exc) {
            throw new IOException("Could not create relation table", exc);
        }
    }

    /**
     * Part of {@link #writeAnnotationHeader(TableRowMetadata)}: Writes first table, containing the primary elements.
     * @param elementTableName The name of the table to create.
     * @return The column name of the primary key elements.
     * @throws IOException If the table could not be created.
     */
    private String createElementsTable(String elementTableName) throws IOException {
        StringBuffer sqlCreate = new StringBuffer("CREATE TABLE ");
        sqlCreate.append(elementTableName);
        sqlCreate.append(" (");
        sqlCreate.append("ID INTEGER PRIMARY KEY");
        
        sqlCreate.append(", ");
        String columnName = sqlifyIdentifier(tableName, "Element");
        sqlCreate.append(columnName);
        sqlCreate.append(" TEXT,");
        sqlCreate.append(" UNIQUE(");
        sqlCreate.append(columnName);
        sqlCreate.append(")");
        
        sqlCreate.append(");");
        
        try {
            PreparedStatement sqlStatement = con.prepareStatement(sqlCreate.toString());
            sqlStatement.execute();
        } catch (SQLException exc) {
            throw new IOException("Could not create element table", exc);
        }
        
        return columnName;
    }
    
    @Override
    protected void writeAnnotationObject(@NonNull TableRowMetadata metadata, @NonNull Object object)
            throws IOException, IllegalArgumentException {
        if (!metadata.isRelation()) {
            // normal handling for non-relations
            super.writeAnnotationObject(metadata, object);
            return;
        }
        
        if (!metadata.isSameClass(object)) {
            throw new IllegalArgumentException("Incompatible type of row passed to writeRow(): "
                    + object.getClass().getName());
        }
        
        // special handling for relations
        
        try {
            Object[] values = metadata.getContent(object);
            
            String[] strings = new String[values.length];
            for (int i = 0; i < values.length; i++) {
                strings[i] = values[i] != null ? values[i].toString() : "";
            }
            
            sqlInsertStatement1.setString(1, strings[0]);
            sqlInsertStatement1.execute();
            sqlInsertStatement1.setString(1, strings[1]);
            sqlInsertStatement1.execute();
            
            for (int i = 0; i < strings.length; i++) {
                sqlInsertStatement2.setString(i + 1, strings[i]);
            }
            sqlInsertStatement2.execute();
            
        } catch (ReflectiveOperationException e) {
            throw new IOException(e);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

}
