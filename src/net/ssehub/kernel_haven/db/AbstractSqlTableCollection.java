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

/**
 * Base class for specifying JDBC-based readers and writers.
 * 
 * @author El-Sharkawy
 */
public abstract class AbstractSqlTableCollection implements ITableCollection {
    
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
     * Converts the given string to a legal SQL identifier.
     * 
     * @param identifier The identifier to convert to a guaranteed legal identifier.
     * 
     * @return A legal identifier based on the provided string.
     */
    public static @NonNull String sqlifyIdentifier(@NonNull String identifier) {
        return '"' + identifier + '"';
    }
    
}
