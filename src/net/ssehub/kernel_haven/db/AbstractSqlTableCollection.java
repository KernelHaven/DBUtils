package net.ssehub.kernel_haven.db;

import static net.ssehub.kernel_haven.util.null_checks.NullHelpers.notNull;

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
 * @author Adam
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
                tables.add(notNull(rs.getString("TABLE_NAME")));
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
     * Escapes the given SQL identifier by adding quotes around it.
     * 
     * @param identifier The identifier to escape.
     * 
     * @return The escaped identifier.
     */
    public static @NonNull String escapeSqlIdentifier(@NonNull String identifier) {
        StringBuilder str = new StringBuilder(identifier);
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == '"') {
                str.insert(i, '"');
                i++; // skip inserted char at next iteration
            }
        }
        
        return '"' + str.toString() + '"';
    }
    
}
