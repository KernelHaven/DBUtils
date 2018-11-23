package net.ssehub.kernel_haven.db;

import java.io.File;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import net.ssehub.kernel_haven.db.sqlite.SqliteCollectionTest;

/**
 * Test suite for this package and sub packages.
 * 
 * @author El-Sharkawy
 */
@RunWith(Suite.class)
@SuiteClasses({
    SqliteCollectionTest.class
    })
public class AllTests {
    
    public static final File TESTDATA = new File("testdata");

}
