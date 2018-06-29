package net.ssehub.kernel_haven.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import net.ssehub.kernel_haven.util.Logger;
import net.ssehub.kernel_haven.util.io.ITableCollection;
import net.ssehub.kernel_haven.util.null_checks.NonNull;

/**
 * Base class for specifying JDBC-based readers and writers.
 * @author El-Sharkawy
 *
 */
public abstract class AbstractSqlTableCollection implements ITableCollection {
    
    protected static final Logger LOGGER = Logger.get();
    private Connection con;
    
    /**
     * Sole constructor.
     * @param The connection for the collection (used to retrieve meta-information).
     * Each writer/reader should use its own connection.
     */
    protected AbstractSqlTableCollection(Connection con) {
        this.con = con;
    }
    
    /**
     * Returns the name of the database for logging information.
     * @return The name of the database to identify it in logging messages.
     */
    protected abstract @NonNull String getTableName();

    @Override
    public @NonNull Set<@NonNull String> getTableNames() throws IOException {
        @NonNull Set<@NonNull String> tables = new HashSet<>();
        
        if (null != con) {
        try {
            DatabaseMetaData md = con.getMetaData();
            ResultSet rs = md.getTables(null, null, "%", null);
            while (rs.next()) {
                tables.add(rs.getString(3));
            }
        } catch (SQLException exc) {
            LOGGER.logException("Could not determine tables names for: " + getTableName(), exc);
        }
        } else {
            LOGGER.logWarning("Could not determine table names, since database connection closed for db: ",
                getTableName());
        }
        
        return tables;
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
}
