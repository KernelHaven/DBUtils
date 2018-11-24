package net.ssehub.kernel_haven.db.sqlite;

import static net.ssehub.kernel_haven.util.null_checks.NullHelpers.notNull;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConfig.Encoding;
import org.sqlite.SQLiteConfig.SynchronousMode;

import net.ssehub.kernel_haven.config.Configuration;
import net.ssehub.kernel_haven.db.AbstractSqlTableCollection;
import net.ssehub.kernel_haven.util.io.ITableCollection;
import net.ssehub.kernel_haven.util.io.TableCollectionReaderFactory;
import net.ssehub.kernel_haven.util.io.TableCollectionWriterFactory;
import net.ssehub.kernel_haven.util.null_checks.NonNull;

/**
 * {@link ITableCollection} to provide read/write access to a (local) SQLite database.
 * 
 * @author El-Sharkawy
 * @author Adam
 */
public class SqliteCollection extends AbstractSqlTableCollection {
    
    static {
        // this static block is invoked by the infrastructure, see loadClasses.txt
        
        // register to TableCollectionReaderFactory
        TableCollectionReaderFactory.INSTANCE.registerHandler("sqlite", SqliteCollection.class);
        
        // register to TableCollectionWriterFactory
        TableCollectionWriterFactory.INSTANCE.registerHandler("sqlite", SqliteCollection.class);
    }
    
    static final @NonNull String ID_FIELD = "ID";
    
    static final @NonNull String ID_FIELD_ESCAPED = escapeSqlIdentifier(ID_FIELD);
    
    private @NonNull File dbFile;
    
    /**
     * Creates a collection for the given database file.
     * 
     * @param dbFile The file to be read/written.
     * 
     * @throws IOException If the given database file could not be opened.
     */
    public SqliteCollection(@NonNull File dbFile) throws IOException {
        super(createConnection(dbFile));
        this.dbFile = dbFile;
    }
    
    /**
     * Creates a new connection to the given database.
     * 
     * @param dbFile The file for which the connections shall be opened.
     * 
     * @return The created connection.
     * 
     * @throws IOException If opening the connection fails. 
     */
    private static @NonNull Connection createConnection(@NonNull File dbFile) throws IOException {
        Connection con;
        String url = "jdbc:sqlite:" + dbFile.getPath();
        
        try {
            SQLiteConfig config = new SQLiteConfig();
            config.enforceForeignKeys(true);
            config.setSynchronous(SynchronousMode.OFF);
            config.setEncoding(Encoding.UTF_8);
            
            con = notNull(config.createConnection(url));
            
        } catch (SQLException exc) {
            throw new IOException("Could not establish connection to: " + dbFile.getPath(), exc);
        }
        
        return con;
    }
    
    @Override
    public @NonNull SqliteReader getReader(@NonNull String name) throws IOException {
        Connection con = createConnection(dbFile);
        try {
            return new SqliteReader(con, getDbName(), name);
            
        } catch (IOException e) {
            // make sure that the new connection is closed if we were not able to create the reader
            try {
                con.close();
            } catch (SQLException e1) {
                // ignore and only throw previous error
            }
            throw e;
        }
    }

    @Override
    public @NonNull SqliteWriter getWriter(@NonNull String name) throws IOException {
        Connection con = createConnection(dbFile);
        return new SqliteWriter(con, getDbName(), name);
    }

    @Override
    public @NonNull Set<@NonNull File> getFiles() throws IOException {
        Set<@NonNull File> files = new HashSet<>(1);
        files.add(dbFile);
        
        return files;
    }

    @Override
    protected @NonNull String getDbName() {
        return notNull(dbFile.getPath());
    }
    
    /**
     * Initialization method called by KernelHaven. See loadClasses.txt
     * 
     * @param config The global pipeline configuration.
     */
    public static void initialize(@NonNull Configuration config) {
        // everything already done in the static block
    }

}
