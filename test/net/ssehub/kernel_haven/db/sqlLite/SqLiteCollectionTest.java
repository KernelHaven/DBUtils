package net.ssehub.kernel_haven.db.sqlLite;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import net.ssehub.kernel_haven.AllTests;
import net.ssehub.kernel_haven.util.io.ITableCollection;
import net.ssehub.kernel_haven.util.io.ITableReader;
import net.ssehub.kernel_haven.util.io.ITableWriter;
import net.ssehub.kernel_haven.util.io.TableElement;
import net.ssehub.kernel_haven.util.io.TableRow;

/**
 * Tests the {@link SqLiteCollection}.
 * @author El-Sharkawy
 *
 */
public class SqLiteCollectionTest {
    
    /**
     * Used for writing/reading data into a SQLite DB.
     * @author El-Sharkawy
     *
     */
    @TableRow
    public static class TestData {
        
        private String name;
        private String value;

        /**
         * Sole constructor.
         * @param name First column
         * @param value Second column
         */
        private TestData(String name, String value) {
            this.name = name;
            this.value = value;
        }
        
        /**
         * First column.
         * @return First column.
         */
        @TableElement(index = 1, name = "name")
        public String getName() {
            return name;
        }
        
        /**
         * Second column.
         * @return Second column.
         */
        @TableElement(index = 2, name = "value")
        public String getValue() {
            return value;
        }
    }

    /**
     * Adds two entries to the DB and reads them to verify whether they are written/read correctly.
     */
    @Test
    public void testWriteAndRead() {
        // Delete generated file at the beginning of the test to allow debugging of the DB.
        File tmpFile = new File(AllTests.TESTDATA, "testWriteAndRead.sqlite");
        if (tmpFile.exists()) {
            tmpFile.delete();
        }
        
        ITableReader reader = null;
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile);
            ITableWriter writer = sqLiteDB.getWriter("Test")) {
            
            TestData elem1 = new TestData("element1", "value1");
            TestData elem2 = new TestData("element2", "value2");
            writer.writeObject(elem1);
            writer.writeObject(elem2);
            writer.close();
            
            reader = sqLiteDB.getReader("Test");
            String[][] fullContent = reader.readFull();
            
            Assert.assertEquals("Unexpected number of rows", 2, fullContent.length);
            // Element 1
            String[] row = fullContent[0];
            Assert.assertEquals("Unexpected value in cell [0, 0]", elem1.getName(), row[0]);
            Assert.assertEquals("Unexpected value in cell [0, 1]", elem1.getValue(), row[1]);
            
            // Element 2
            row = fullContent[1];
            Assert.assertEquals("Unexpected value in cell [1, 0]", elem2.getName(), row[0]);
            Assert.assertEquals("Unexpected value in cell [1, 1]", elem2.getValue(), row[1]);
            
        } catch (IOException exc) {
            Assert.fail(exc.toString());
        } finally {
            if (null != reader) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * A test data class.
     */
    @TableRow(isRelation = true)
    public static class RelationData {
        
        private String feature;
        
        private String dependsOn;

        /**
         * Creates this object.
         * @param feature Value 1.
         * @param dependsOn Value 2.
         */
        public RelationData(String feature, String dependsOn) {
            this.feature = feature;
            this.dependsOn = dependsOn;
        }

        /**
         * Value 1.
         * 
         * @return Value 1.
         */
        @TableElement(index = 1, name = "Feature")
        public String getFeature() {
            return feature;
        }
        
        /**
         * Value 2.
         * 
         * @return Value 2.
         */
        @TableElement(index = 2, name = "Depends On")
        public String getDependsOn() {
            return dependsOn;
        }
        
    }
    
    /**
     * Tests writing of a relation structure.
     * 
     * @throws IOException unwanted.
     */
    @Test
    public void testRelationStructure() throws IOException {
        // Delete generated file at the beginning of the test to allow debugging of the DB.
        File tmpFile = new File(AllTests.TESTDATA, "testRelationStructure.sqlite");
        if (tmpFile.exists()) {
            tmpFile.delete();
        }
        
        try (SqLiteCollection collection = new SqLiteCollection(tmpFile)) {
            
            try (ITableWriter writer = collection.getWriter("Feature Dependencies")) {
                writer.writeObject(new RelationData("A", "B"));
                writer.writeObject(new RelationData("A", "C"));
                writer.writeObject(new RelationData("B", "C"));
            }
            
        }
    }

}
