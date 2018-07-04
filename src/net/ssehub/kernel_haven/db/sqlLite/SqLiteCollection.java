package net.ssehub.kernel_haven.db.sqlLite;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import net.ssehub.kernel_haven.db.AbstractSqlTableCollection;
import net.ssehub.kernel_haven.util.io.ITableCollection;
import net.ssehub.kernel_haven.util.io.ITableReader;
import net.ssehub.kernel_haven.util.io.ITableWriter;
import net.ssehub.kernel_haven.util.io.TableCollectionReaderFactory;
import net.ssehub.kernel_haven.util.io.TableCollectionWriterFactory;
import net.ssehub.kernel_haven.util.null_checks.NonNull;
import net.ssehub.kernel_haven.util.null_checks.Nullable;

/**
 * {@link ITableCollection} to provide read/write access to a (local) SQLite database.
 * @author El-Sharkawy
 *
 */
public class SqLiteCollection extends AbstractSqlTableCollection {
    
    static {
        // this static block is invoked by the infrastructure, see loadClasses.txt
        
        // register to TableCollectionReaderFactory
        TableCollectionReaderFactory.INSTANCE.registerHandler("sqlite", SqLiteCollection.class);
        
        // register to TableCollectionWriterFactory
        TableCollectionWriterFactory.INSTANCE.registerHandler("sqlite", SqLiteCollection.class);
    }
    
    private @NonNull File dbFile;
    
    /**
     * Sole constructor.
     * @param dbFile The file to be read/written.
     */
    public SqLiteCollection(@NonNull File dbFile) {
        super(createConnection(dbFile));
        this.dbFile = dbFile;
    }
    
    /**
     * Creates a new connection to the data base.
     * @param dbFile The file for which the connections shall be opened.
     * @return The connection or <tt>null</tt> in case of errors.
     */
    private static @Nullable Connection createConnection(File dbFile) {
        Connection con = null;
        String url = "jdbc:sqlite:" + dbFile.getAbsolutePath();
        try {
            // DB parameters: Create a connection to the database
            con = DriverManager.getConnection(url);
            LOGGER.logDebug2("SQLite connection has been established for file: ", dbFile.getAbsolutePath());
        } catch (SQLException exc) {
            LOGGER.logException("Could not establish connection to: " + dbFile.getAbsolutePath(), exc);
        }
        
        return con;
    }
    
    @Override
    public @NonNull ITableReader getReader(@NonNull String name) throws IOException {
        Connection con = createConnection(dbFile);
        if (null == con) {
            throw new IOException("Could not create connection for database: " + getTableName());
        }
        
        return new SqLiteReader(con, getTableName(), name);
    }

    @Override
    public @NonNull ITableWriter getWriter(@NonNull String name) throws IOException {
        Connection con = createConnection(dbFile);
        if (null == con) {
            throw new IOException("Could not create connection for database: " + getTableName());
        }
        
        return new SqLiteWriter(con, getTableName(), name);
    }

    @Override
    public @NonNull Set<@NonNull File> getFiles() throws IOException {
        @NonNull Set<@NonNull File> files = new HashSet<>(1);
        files.add(dbFile);
        
        return files;
    }

    @Override
    protected @NonNull String getTableName() {
        return dbFile.getAbsolutePath();
    }

}
