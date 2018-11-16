package net.ssehub.kernel_haven.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import net.ssehub.kernel_haven.util.io.ITableCollection;
import net.ssehub.kernel_haven.util.null_checks.NonNull;
import net.ssehub.kernel_haven.util.null_checks.Nullable;

/**
 * Base class for specifying JDBC-based readers and writers.
 * 
 * @author El-Sharkawy
 */
public abstract class AbstractSqlTableCollection implements ITableCollection {
    
    public static final boolean OLD_STYLE_IDENTIFIER_SQLIFY = true;
    
    private @NonNull Connection con;
    
    /**
     * Sole constructor.
     * @param con The connection for the collection (used to retrieve meta-information).
     * Each writer/reader should use its own connection.
     */
    protected AbstractSqlTableCollection(@NonNull Connection con) {
        this.con = con;
    }
    
    /**
     * Returns the name of the database for logging information.
     * 
     * @return The name of the database to identify it in logging messages.
     */
    protected abstract @NonNull String getDbName();

    @Override
    public @NonNull Set<@NonNull String> getTableNames() throws IOException {
        Set<@NonNull String> tables = new HashSet<>();
        
        try {
            DatabaseMetaData md = con.getMetaData();
            ResultSet rs = md.getTables(null, null, "%", null);
            while (rs.next()) {
                tables.add(rs.getString(3));
            }
        } catch (SQLException exc) {
            throw new IOException("Could not determine tables names for: " + getDbName(), exc);
        }
        
        return tables;
    }
    
    @Override
    public void close() throws IOException {
        try {
            con.close();
        } catch (SQLException exc) {
            throw new IOException("Could not close connection for: " + getDbName(), exc);
        }
    }
    
    /**
     * The old implementation that did string-replacing to ensure valid identifier names.
     * 
     * @param identifier The identifier to escape.
     * 
     * @return An identifier that is guaranteed to be valid SQL.
     */
    private static @NonNull String oldSqlifyIdentifier(@NonNull String identifier) {
        StringBuilder result = new StringBuilder(identifier);
        for (int i = 0; i < result.length(); i++) {
            char character = result.charAt(i);
            boolean smallChar = (character >= 'A' && character <= 'Z');
            boolean bigChar = (character >= 'a' && character <= 'z');
            boolean number = (character >= '0' && character <= '9') && i != 0; // number only allowed after first pos
            if (!smallChar && !bigChar && !number) {
                result.setCharAt(i, '_');
            }
        }
        
        return result.toString();
    }
    
    /**
     * Escapes the given SQL identifier by adding quotes around it.
     * 
     * @param identifier The identifier to escape.
     * 
     * @return The escaped identifier.
     */
    public static @NonNull String escapeSqlIdentifier(@NonNull String identifier) {
        return '"' + identifier + '"'; // TODO: escape " inside the string
    }
    
    /**
     * Converts the given string to a legal SQL identifier.
     * 
     * @param tableName The table name that the identifier is used in.
     * @param columName The column name in the table that is used as the identifier. If <code>null</code>, then
     *      tableName is converted to the legal identifier name.
     * 
     * @return A legal identifier based on the provided string.
     */
    public static @NonNull String sqlifyIdentifier(@NonNull String tableName, @Nullable String columName) {
        if (OLD_STYLE_IDENTIFIER_SQLIFY) {
            if (columName != null) {
                // for column names, the old style prefixed the table name
                columName = tableName + '_' + columName;
            } else {
                columName = tableName;
            }
            columName = oldSqlifyIdentifier(columName);
        }
        
        return escapeSqlIdentifier(columName != null ? columName : tableName);
    }
    
}
