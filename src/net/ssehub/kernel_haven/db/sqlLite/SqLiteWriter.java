package net.ssehub.kernel_haven.db.sqlLite;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import net.ssehub.kernel_haven.util.Logger;
import net.ssehub.kernel_haven.util.io.AbstractTableWriter;
import net.ssehub.kernel_haven.util.io.TableRowMetadata;
import net.ssehub.kernel_haven.util.null_checks.NonNull;
import net.ssehub.kernel_haven.util.null_checks.Nullable;

/**
 * Writes elements into a (local) SQLite database.
 * @author El-Sharkawy
 *
 */
public class SqLiteWriter extends AbstractTableWriter {
    
    private static final Logger LOGGER = Logger.get();
    
    private Connection con;
    private String dbName;
    private String tableName;
    
    // Normal writing
    private PreparedStatement sqlInsertStatement;
    private String sqlInsertQuery;
    
    // relation writing
    private PreparedStatement sqlInsertStatement1;
    private PreparedStatement sqlInsertStatement2;
    
    /**
     * Sole constructor.
     * @param con A connection, exclusively used by this writer.
     * @param dbName The name of the database (used in log messages only).
     * @param tableName The name of the table to be created inside the database.
     */
    SqLiteWriter(Connection con, String dbName, String tableName) {
        this.con = con;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    @Override
    public void close() throws IOException {
        try {
            if (con != null) {
                con.close();
            }
        } catch (SQLException exc) {
            LOGGER.logException("Could not close connection for: " + getTableName(), exc);
        }
    }
    
    @Override
    public void writeHeader(@Nullable Object /*@NonNull*/ ... fields) throws IOException {
        // Create tables
        if (null != fields && fields.length > 0) {
            // Create the table containing the element definitions
            StringBuffer sqlCreate = new StringBuffer("CREATE TABLE ");
            sqlCreate.append(tableName);
            sqlCreate.append(" (");
            sqlCreate.append("ID INTEGER PRIMARY KEY");
            
            StringBuffer sqlInsert = new StringBuffer("INSERT INTO ");
            sqlInsert.append(tableName);
            sqlInsert.append(" VALUES (");
            sqlInsert.append("NULL");
            
            for (int i = 0; i < fields.length; i++) {
                /*
                 * Avoid illegal table names
                 * Column names cannot be prepared: https://stackoverflow.com/a/27041304
                 */
                sqlCreate.append(", ");
                String columnName = tableName + "_" + fields[i].toString();
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
                LOGGER.logException("Could not prepare SQL queries.", exc);
            }
        }
    }
    
    /**
     * Avoids illegal column names, not supported by SQL.
     * @param columnName The name to verify its correct name.
     * @return A legal name based on the provided instance, this will be a new instance in any case.
     */
    private @NonNull String sqlifyColumnName(@NonNull String columnName) {
        StringBuffer result = new StringBuffer(columnName.length());
        for (int i = 0; i < columnName.length(); i++) {
            switch (columnName.charAt(i)) {
            case ' ':
                result.append('_');
                break;
            
            // Escape German umlauts
            case 'ä':
                result.append("ae");
                break;
            case 'Ä':
                result.append("Ae");
                break;
            case 'ö':
                result.append("oe");
                break;
            case 'Ö':
                result.append("Oe");
                break;
            case 'ü':
                result.append("ue");
                break;
            case 'Ü':
                result.append("Ue");
                break;
            case 'ß':
                result.append("ss");
                break;
            default:
                result.append(columnName.charAt(i));
                break;
            }
        }
        
        return result.toString();
    }
    
    @Override
    public void writeRow(@Nullable Object... columns) throws IOException {
        if (null != columns) {
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
    }

    @Override
    public void flush() throws IOException {
        // No action required, all data is written directly into database.
    }
    
    /**
     * Returns the name of the database and the table for logging information.
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
        String elementTableName = sqlifyColumnName(tableName) + "_Elements";
        
        StringBuffer sqlCreate = new StringBuffer("CREATE TABLE ");
        sqlCreate.append(elementTableName);
        sqlCreate.append(" (");
        sqlCreate.append("ID INTEGER PRIMARY KEY");
        
        /*
         * Avoid illegal table names
         * Column names cannot be prepared: https://stackoverflow.com/a/27041304
         */
        sqlCreate.append(", ");
        String columnName = sqlifyColumnName(tableName + "_Element");
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
        
        @NonNull Object[] headers = metadata.getHeaders();
        if (headers.length != 2) {
            throw new IllegalArgumentException("Relation TableRows must have exactly 2 TableElements");
        }
        
        sqlCreate = new StringBuffer("CREATE TABLE ");
        sqlCreate.append(sqlifyColumnName(tableName));
        sqlCreate.append(" (");
        
        sqlCreate.append(sqlifyColumnName(headers[0].toString()));
        sqlCreate.append(" INTEGER,");
        
        sqlCreate.append(sqlifyColumnName(headers[1].toString()));
        sqlCreate.append(" INTEGER,");
        
        sqlCreate.append(" FOREIGN KEY(");
        sqlCreate.append(sqlifyColumnName(headers[0].toString()));
        sqlCreate.append(") REFERENCES ");
        sqlCreate.append(elementTableName);
        sqlCreate.append(" (ID),");
        
        sqlCreate.append(" FOREIGN KEY(");
        sqlCreate.append(sqlifyColumnName(headers[1].toString()));
        sqlCreate.append(") REFERENCES ");
        sqlCreate.append(elementTableName);
        sqlCreate.append("s (ID)");
        
        sqlCreate.append(");");
        
        try {
            PreparedStatement sqlStatement = con.prepareStatement(sqlCreate.toString());
            sqlStatement.execute();
        } catch (SQLException exc) {
            throw new IOException("Could not create relation table", exc);
        }
        
        StringBuffer sqlInsert1 = new StringBuffer();
        sqlInsert1.append("INSERT OR IGNORE INTO ");
        sqlInsert1.append(elementTableName);
        sqlInsert1.append(" VALUES (NULL, ?); ");
        
        StringBuffer sqlInsert2 = new StringBuffer();
        sqlInsert2.append("INSERT INTO ");
        sqlInsert2.append(sqlifyColumnName(tableName));
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
        
        sqlInsert2.append(");");
        
        try {
            sqlInsertStatement1 = con.prepareStatement(sqlInsert1.toString());
            sqlInsertStatement2 = con.prepareStatement(sqlInsert2.toString());
        } catch (SQLException exc) {
            throw new IOException("Could not prepare insert statement", exc);
        }
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
            
            String[] strings = {
                    values[0] != null ? values[0].toString() : "",
                            values[1] != null ? values[1].toString() : ""
            };
            
            sqlInsertStatement1.setString(1, strings[0]);
            sqlInsertStatement1.execute();
            sqlInsertStatement1.setString(1, strings[1]);
            sqlInsertStatement1.execute();
            
            sqlInsertStatement2.setString(1, strings[0]);
            sqlInsertStatement2.setString(2, strings[1]);
            sqlInsertStatement2.execute();
            
        } catch (ReflectiveOperationException e) {
            throw new IOException(e);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

}
