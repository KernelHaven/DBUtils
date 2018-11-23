package net.ssehub.kernel_haven.db.sqlLite;

import static net.ssehub.kernel_haven.db.AbstractSqlTableCollection.escapeSqlIdentifier;
import static net.ssehub.kernel_haven.db.AbstractSqlTableCollection.sqlifyIdentifier;
import static net.ssehub.kernel_haven.db.sqlLite.SqLiteCollection.ID_FIELD_ESCAPED;
import static net.ssehub.kernel_haven.util.null_checks.NullHelpers.notNull;

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
     */
    SqLiteWriter(@NonNull Connection con, @NonNull String dbName, @NonNull String tableName) {
        this.con = con;
        this.dbName = dbName;
        this.tableName = tableName;
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
        if (sqlInsertStatement1 != null || sqlInsertStatement2 != null) {
            throw new IOException("Can't mix writeObject() and writeHeader()");
        }
        if (sqlInsertStatement != null) {
            throw new IOException("Can't call writeHeader() twice");
        }
        if (fields.length < 1) {
            throw new IOException("Can't create a table with no columns");
        }

        StringBuilder headersString = new StringBuilder();
        StringBuilder insertQuestionMarks = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (fields[i] == null) {
                throw new IOException("Table header null is not allowed");
            }
            
            headersString.append(String.format(", %s TEXT",
                    sqlifyIdentifier(tableName, notNull(fields[i]).toString())));
            insertQuestionMarks.append(", ?");
        }
        
        String escapedTableName = sqlifyIdentifier(tableName, null); 
        
        String sqlDrop = String.format("DROP TABLE IF EXISTS %s;",
                escapedTableName);
        
        String sqlCreate = String.format("CREATE TABLE %s (%s INTEGER PRIMAR KEY %s);",
                escapedTableName,
                ID_FIELD_ESCAPED,
                headersString.toString());
        
        // use NULL for primary key, so it is auto-incremented
        String sqlInsert = String.format("INSERT INTO %s VALUES (NULL %s);",
                escapedTableName,
                insertQuestionMarks);
        
        try {
            con.prepareStatement(sqlDrop).execute();
            con.prepareStatement(sqlCreate).execute();
        } catch (SQLException exc) {
            throw new IOException("Could not create table", exc);
        }
         
        try {
            sqlInsertStatement = con.prepareStatement(sqlInsert);
            sqlInsertQuery = sqlInsert;
        } catch (SQLException exc) {
            throw new IOException("Could not prepare SQL insert query", exc);
        }
    }
    
    @Override
    public void writeRow(@Nullable Object /*@NonNull*/ ... columns) throws IOException {
        if (sqlInsertStatement == null) {
            throw new IOException("writeHeader() must be called before writeRow()");
        }
        
        try {
            int expectedParamCount = sqlInsertStatement.getParameterMetaData().getParameterCount();
            if (columns.length != expectedParamCount) {
                throw new IOException("Expected " + expectedParamCount + " elements, but got " + columns.length);
            }
        } catch (SQLException e) {
            throw new IOException("Can't check insert statement", e);
        }
        
        for (int i = 0; i < columns.length; i++) {
            try {
                String value = columns[i] != null ? notNull(columns[i]).toString() : null;
                sqlInsertStatement.setString((i + 1), value);
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
        
        if (sqlInsertStatement != null) {
            throw new IOException("Can't mix writeHeader() and writeObject()");
        }
        
        // special handling for relations
        // create two tables: one ID content mapping, and one relation mapping (ID, ID)
        String escapedTableName = sqlifyIdentifier(tableName, null);
        String elementTableName = sqlifyIdentifier(tableName + " Elements", null);
        
        @NonNull Object[] headers = metadata.getHeaders();
        if (headers.length < 2) {
            throw new IllegalArgumentException("Relation TableRows must have at least 2 TableElements");
        }
        
        String pkColumnName = createElementsTable(elementTableName);
        createRelationTable(elementTableName, headers);
        
        // Create view combining the two tables
        createView(elementTableName, pkColumnName, headers);
        
        String sqlInsert1 = String.format("INSERT OR IGNORE INTO %s VALUES (NULL, ?);",
                elementTableName);
        
        StringBuilder insertQuestionMarks = new StringBuilder();
        for (int i = 2; i < headers.length; i++) {
            insertQuestionMarks.append(", ?");
        }
        
        String idQuery = String.format("(SELECT %s FROM %s WHERE %s = ?)",
                ID_FIELD_ESCAPED,
                elementTableName,
                pkColumnName);
        
        String sqlInsert2 = String.format("INSERT INTO %1$s VALUES (%2$s, %2$s %3$s);",
                escapedTableName,
                idQuery,
                insertQuestionMarks.toString());
        
        try {
            sqlInsertStatement1 = con.prepareStatement(sqlInsert1);
            sqlInsertStatement2 = con.prepareStatement(sqlInsert2.toString());
        } catch (SQLException exc) {
            throw new IOException("Could not prepare insert statements", exc);
        }
    }

    /**
     * Part of {@link #writeAnnotationHeader(TableRowMetadata)}: Creates a view combining both tables.
     * @param elementTableName The name of the first table, which is used to reference elements as foreign key.
     * @param columnName The key column of the first table
     * @param headers A at least 2-dim array containing the names of the relationships.
     * @throws IOException If the view could not be created.
     */
    private void createView(@NonNull String elementTableName, @NonNull String columnName,
            @NonNull Object @NonNull [] headers) throws IOException {
        
        // Handling of optional, non-relational elements starting at 3rd index of header
        StringBuffer optionalColumns = new StringBuffer();
        if (headers.length > 2) {
            for (int i = 2; i < headers.length; i++) {
                optionalColumns
                    .append(", ")
                    .append(sqlifyIdentifier(tableName, headers[i].toString()));
            }
        }
        
        String escapedFirstHeader = sqlifyIdentifier(tableName, headers[0].toString()); 
        String escapedSecondHeader = sqlifyIdentifier(tableName, headers[1].toString()); 
        
        /*
         * Join twice with the element table, using two temporary names
         * The first join is used to resolve the first foreign key reference
         * The second join is used to resolve the second foreign key reference
         */
        String tmp1 = escapeSqlIdentifier("tmp_join1");
        String tmp2 = escapeSqlIdentifier("tmp_join2");
        
        String select = String.format(
                "SELECT %1$s.%3$s AS %4$s, %2$s.%3$s AS %5$s %6$s FROM %7$s"
                + " INNER JOIN %8$s AS %1$s ON %7$s.%4$s = %1$s.%9$s"
                + " INNER JOIN %8$s AS %2$s ON %7$s.%5$s = %2$s.%9$s;",
                
                /*1$*/ tmp1,
                /*2$*/ tmp2,
                /*3$*/ columnName,
                /*4$*/ escapedFirstHeader,
                /*5$*/ escapedSecondHeader,
                /*6$*/ optionalColumns,
                /*7$*/ sqlifyIdentifier(tableName, null),
                /*8$*/ elementTableName,
                /*9$*/ ID_FIELD_ESCAPED
        );
        
        String sqlCreateView = String.format("CREATE VIEW IF NOT EXISTS %s AS %s;",
                sqlifyIdentifier(tableName + " View", null),
                select);
        
        try {
            con.prepareStatement(sqlCreateView).execute();
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
    private void createRelationTable(@NonNull String elementTableName, @NonNull Object @NonNull [] headers)
            throws IOException {
        
        String escapedFirstHeader = sqlifyIdentifier(tableName, headers[0].toString());
        String escapedSecondHeader = sqlifyIdentifier(tableName, headers[1].toString());
        
        StringBuilder extraColumns = new StringBuilder();
        for (int i = 2; i < headers.length; i++) {
            extraColumns.append(String.format("%s TEXT, ",
                    sqlifyIdentifier(tableName, headers[i].toString())));
        }
        
        String sqlCreate = String.format(
                "CREATE TABLE %1$s (%2$s INTEGER, %3$s INTEGER, %4$s "
                + "FOREIGN KEY(%2$s) REFERENCES %5$s(%6$s), FOREIGN KEY(%3$s) REFERENCES %5$s(%6$s));",
                
                /*1$*/ sqlifyIdentifier(tableName, null),
                /*2$*/ escapedFirstHeader,
                /*3$*/ escapedSecondHeader,
                /*4$*/ extraColumns,
                /*5$*/ elementTableName,
                /*6$*/ ID_FIELD_ESCAPED
        );
        
        try {
            con.prepareStatement(sqlCreate).execute();
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
    private @NonNull String createElementsTable(@NonNull String elementTableName) throws IOException {
        
        String columnName = sqlifyIdentifier(tableName, "Element");
        
        String sqlCreate = String.format(
                "CREATE TABLE %1$s (%2$s INTEGER PRIMARY KEY, %3$s TEXT, UNIQUE(%3$s));",
                
                /*1$*/ elementTableName,
                /*2$*/ ID_FIELD_ESCAPED,
                /*3$*/ columnName
        );
        
        try {
            con.prepareStatement(sqlCreate).execute();
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
                strings[i] = values[i] != null ? values[i].toString() : null;
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
