package net.ssehub.kernel_haven.db.sqlLite;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            
            assertContent(fullContent, elem1, elem2);
            
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
     * Tests whether illegal table names are escaped correctly.
     */
    @Test
    public void testEscapedTableName() {
        // Delete generated file at the beginning of the test to allow debugging of the DB.
        File tmpFile = new File(AllTests.TESTDATA, "testEscapedTableName.sqlite");
        if (tmpFile.exists()) {
            tmpFile.delete();
        }
        
        ITableReader reader = null;
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile);
            ITableWriter writer = sqLiteDB.getWriter("Test Table")) {
            
            TestData elem1 = new TestData("element1", "value1");
            TestData elem2 = new TestData("element2", "value2");
            writer.writeObject(elem1);
            writer.writeObject(elem2);
            writer.close();
            
            reader = sqLiteDB.getReader("Test Table");
            String[][] fullContent = reader.readFull();
            
            assertContent(fullContent, elem1, elem2);
            
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
     * Checks whether the read content matches to the expected (written) {@link TestData}.
     * @param fullContent The read (asserted) content.
     * @param expectedElements The written (expected) elements. Must have the same size as <tt>fullContent</tt>.
     */
    private void assertContent(String[][] fullContent, TestData... expectedElements) {
        Assert.assertEquals("Unexpected number of rows", expectedElements.length, fullContent.length);
        
        int rowIndex = 0;
        for (String[] row : fullContent) {
            Assert.assertEquals("Unexpected value in cell [0, 0]", expectedElements[rowIndex].getName(), row[0]);
            Assert.assertEquals("Unexpected value in cell [0, 1]", expectedElements[rowIndex].getValue(), row[1]);
            rowIndex++;
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
     * A test data class.
     */
    @TableRow(isRelation = true)
    public static class RelationDataWithExtraElement {
        
        private String feature;
        
        private String dependsOn;
        
        private String context;

        /**
         * Creates this object.
         * 
         * @param feature Value 1.
         * @param dependsOn Value 2.
         * @param context Value 3.
         */
        public RelationDataWithExtraElement(String feature, String dependsOn, String context) {
            this.feature = feature;
            this.dependsOn = dependsOn;
            this.context = context;
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
        
        /**
         * Value 3.
         * 
         * @return Value 3.
         */
        @TableElement(index = 3, name = "Context")
        public String getContext() {
            return context;
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
        Assert.assertFalse("Test DB exist before test (but should be created during testing: "
            + tmpFile.getAbsolutePath(), tmpFile.exists());
        
        try (SqLiteCollection collection = new SqLiteCollection(tmpFile)) {
            
            try (ITableWriter writer = collection.getWriter("Feature Dependencies")) {
                writer.writeObject(new RelationData("A", "B"));
                writer.writeObject(new RelationData("A", "C"));
                writer.writeObject(new RelationData("B", "C"));
            }
            
        }
    }
    
    /**
     * Tests writing of a relation structure with extra data per relation.
     * 
     * @throws IOException unwanted.
     */
    @Test
    public void testRelationStructureWithExtraElement() throws IOException {
        // Delete generated file at the beginning of the test to allow debugging of the DB.
        File tmpFile = new File(AllTests.TESTDATA, "testRelationStructureWithData.sqlite");
        if (tmpFile.exists()) {
            tmpFile.delete();
        }
        Assert.assertFalse("Test DB exist before test (but should be created during testing: "
            + tmpFile.getAbsolutePath(), tmpFile.exists());
        
        try (SqLiteCollection collection = new SqLiteCollection(tmpFile)) {
            
            try (ITableWriter writer = collection.getWriter("Feature Dependencies")) {
                writer.writeObject(new RelationDataWithExtraElement("A", "B", "Context 1"));
                writer.writeObject(new RelationDataWithExtraElement("A", "C", "Context 2"));
                writer.writeObject(new RelationDataWithExtraElement("B", "C", "Context 3"));
            }
            
        }
    }
    
    /**
     * Tests two different king of elements.
     * 
     * @throws IOException unwanted.
     */
    @Test
    public void testHeterogeneousStructure() throws IOException {
        // Test data
        List<TestData> firstDataSet = new ArrayList<>();
        firstDataSet.add(new TestData("A", "A || C"));
        firstDataSet.add(new TestData("B", "C"));
        firstDataSet.add(new TestData("C", "1"));
        
        List<RelationData> secondDataSet = new ArrayList<>();
        secondDataSet.add(new RelationData(firstDataSet.get(0).name, "B"));
        secondDataSet.add(new RelationData(firstDataSet.get(0).name, "C"));
        secondDataSet.add(new RelationData(firstDataSet.get(1).name, "C"));
        
        // Delete generated file at the beginning of the test to allow debugging of the DB.
        File tmpFile = new File(AllTests.TESTDATA, "testHeterogeneousStructure.sqlite");
        if (tmpFile.exists()) {
            tmpFile.delete();
        }
        Assert.assertFalse("Test DB exist before test (but should be created during testing: "
            + tmpFile.getAbsolutePath(), tmpFile.exists());
        
        
        // Write data of two analyses into two tables
        try (SqLiteCollection collection = new SqLiteCollection(tmpFile)) {
            try (ITableWriter writer = collection.getWriter("Features")) {
                for (TestData tData : firstDataSet) {
                    writer.writeObject(tData);
                }
            }
            try (ITableWriter writer = collection.getWriter("Feature Dependencies")) {
                for (RelationData rData : secondDataSet) {
                    writer.writeObject(rData);
                }
            }
        }
        
        // Check if data was written successfully
        try (SqLiteCollection collection = new SqLiteCollection(tmpFile)) {
            try (ITableReader reader = collection.getReader("Features")) {
                String[][] firstContent = reader.readFull();
                Assert.assertEquals(3, firstContent.length);
                for (int i = 0; i < firstContent.length; i++) {
                    Assert.assertEquals(firstContent[i][0], firstDataSet.get(i).name);
                    Assert.assertEquals(firstContent[i][1], firstDataSet.get(i).value);
                }
            }
            // Contents of relation data (isRelation flag) can be retrieved from table_View
            try (ITableReader reader = collection.getReader("Feature_Dependencies_View")) {
                String[][] secondContent = reader.readFull();
                Assert.assertEquals(3, secondContent.length);
                for (int i = 0; i < secondContent.length; i++) {
                    Assert.assertEquals(secondContent[i][0], secondDataSet.get(i).feature);
                    Assert.assertEquals(secondContent[i][1], secondDataSet.get(i).dependsOn);
                }
            }
        }
    }

}
