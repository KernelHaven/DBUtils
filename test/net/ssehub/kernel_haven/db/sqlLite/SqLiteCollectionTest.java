package net.ssehub.kernel_haven.db.sqlLite;

import static net.ssehub.kernel_haven.db.AbstractSqlTableCollection.OLD_STYLE_IDENTIFIER_SQLIFY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import net.ssehub.kernel_haven.db.AllTests;
import net.ssehub.kernel_haven.util.io.ITableCollection;
import net.ssehub.kernel_haven.util.io.ITableReader;
import net.ssehub.kernel_haven.util.io.ITableWriter;
import net.ssehub.kernel_haven.util.io.TableElement;
import net.ssehub.kernel_haven.util.io.TableRow;
import net.ssehub.kernel_haven.util.null_checks.NonNull;

/**
 * Tests the {@link SqLiteCollection}.
 * @author El-Sharkawy
 *
 */
public class SqLiteCollectionTest {
    
    private static final File TMP_DIR = new File(AllTests.TESTDATA, "tmp");
    
    /**
     * Clears all *.sqlite files from the temporary directory.
     * Delete generated files at the beginning of the tests to allow debugging of the DB.
     * 
     * @throws IOException If clearing fails.
     */
    @BeforeClass
    public static void clearTmpDir() throws IOException {
        if (!TMP_DIR.isDirectory()) {
            TMP_DIR.mkdir();
        }
        
        for (File f : TMP_DIR.listFiles()) {
            if (f.getName().endsWith(".sqlite")) {
                f.delete();
            }
        }
        
        assertThat(TMP_DIR.isDirectory(), is(true));
    }
    
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
     * Adds two entries (objects) to the DB and reads them to verify whether they are written/read correctly.
     */
    @Test
    public void testWriteAndReadObject() {
        File tmpFile = new File(TMP_DIR, "testWriteAndReadObject.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        ITableReader reader = null;
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile);
            ITableWriter writer = sqLiteDB.getWriter("Test")) {
            
            TestData elem1 = new TestData("element1", "value1");
            TestData elem2 = new TestData("element2", "value2");
            writer.writeObject(elem1);
            writer.writeObject(elem2);
            writer.close();
            
            reader = sqLiteDB.getReader("Test");
            
            assertContent(reader, "Test", elem1, elem2);
            
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
     * Tests writing and reading a table (like {@link #testWriteAndReadObject()}), but using
     * {@link ITableWriter#writeRow(Object...)} instead of {@link ITableWriter#writeObject(Object)}.
     */
    @Test
    public void testWriteAndReadRows() {
        File tmpFile = new File(TMP_DIR, "testWriteAndReadRows.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        ITableReader reader = null;
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile);
            ITableWriter writer = sqLiteDB.getWriter("Test")) {
            
            writer.writeHeader("First Name", "Last Name");
            writer.writeRow("Donald", "Duck");
            writer.writeRow("Scrooge", "McDuck");
            writer.writeRow("Daisy", "Duck");
            
            writer.close();
            
            reader = sqLiteDB.getReader("Test");
            
            assertThat(reader.getLineNumber(), is(0));
            
            if (OLD_STYLE_IDENTIFIER_SQLIFY) {
                assertThat(reader.readNextRow(), is(new String[] {"Test_First_Name", "Test_Last_Name"}));
            } else {
                assertThat(reader.readNextRow(), is(new String[] {"First Name", "Last Name"}));
            }
            assertThat(reader.getLineNumber(), is(0)); // header is not an SQL row
            
            assertThat(reader.readNextRow(), is(new String[] {"Donald", "Duck"}));
            assertThat(reader.getLineNumber(), is(1));
            assertThat(reader.readNextRow(), is(new String[] {"Scrooge", "McDuck"}));
            assertThat(reader.getLineNumber(), is(2));
            assertThat(reader.readNextRow(), is(new String[] {"Daisy", "Duck"}));
            assertThat(reader.getLineNumber(), is(3));

            assertThat(reader.readNextRow(), nullValue());
            
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
        File tmpFile = new File(TMP_DIR, "testEscapedTableName.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        String tableName = OLD_STYLE_IDENTIFIER_SQLIFY ? "Test_Table" : "Test Table";
        
        ITableReader reader = null;
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile);
            ITableWriter writer = sqLiteDB.getWriter("Test Table")) {
            
            TestData elem1 = new TestData("element1", "value1");
            TestData elem2 = new TestData("element2", "value2");
            writer.writeObject(elem1);
            writer.writeObject(elem2);
            writer.close();
            
            reader = sqLiteDB.getReader(tableName);
            
            assertContent(reader, "Test_Table", elem1, elem2);
            
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
     * 
     * @param reader The reader to get the data from.
     * @param tableName The name of the table the data is in (for old-style column names).
     * @param expectedElements The written (expected) elements. Must have the same size as <tt>fullContent</tt>.
     * 
     * @throws IOException If reading the reader fails.
     */
    private void assertContent(ITableReader reader, String tableName, TestData... expectedElements) throws IOException {
        int rowIndex = 0;
        assertThat(reader.getLineNumber(), is(0));
        
        // read  header
        if (OLD_STYLE_IDENTIFIER_SQLIFY) {
            assertThat(reader.readNextRow(), is(new String[] {tableName + "_name", tableName + "_value"}));
        } else {
            assertThat(reader.readNextRow(), is(new String[] {"name", "value"}));
        }
        assertThat(reader.getLineNumber(), is(0)); // header is not an SQL row
        
        String[] row;
        while ((row = reader.readNextRow()) != null) {
            assertThat(reader.getLineNumber(), is(rowIndex + 1));
            
            String expectedName = expectedElements[rowIndex].getName();
            if (expectedName == null) {
                expectedName = "";
            }
            String expectedValue = expectedElements[rowIndex].getValue();
            if (expectedValue == null) {
                expectedValue = "";
            }
            Assert.assertEquals("Unexpected value in cell [0, 0]", expectedName, row[0]);
            Assert.assertEquals("Unexpected value in cell [0, 1]", expectedValue, row[1]);
            rowIndex++;
        }
        
        if (rowIndex != expectedElements.length) {
            fail("Expected further row, but got null");
        }
        assertThat(reader.getLineNumber(), is(expectedElements.length));
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
        File tmpFile = new File(TMP_DIR, "testRelationStructure.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (SqLiteCollection collection = new SqLiteCollection(tmpFile)) {
            // Write relational data (will create an view)
            try (ITableWriter writer = collection.getWriter("Feature Dependencies")) {
                writer.writeObject(new RelationData("A", "B"));
                writer.writeObject(new RelationData("A", "C"));
                writer.writeObject(new RelationData("B", "C"));
            }
            
            // Test correct creation of view
            String[] expectedDependents = {"A", "A", "B"};
            String[] expectedDependentOns = {"B", "C", "C"};
            
            String tableName = OLD_STYLE_IDENTIFIER_SQLIFY
                    ? "Feature_Dependencies_View" : "Feature Dependencies View";
            try (ITableReader reader = collection.getReader(tableName)) {
                String[][] content = reader.readFull();
                Assert.assertEquals(4, content.length);
                if (OLD_STYLE_IDENTIFIER_SQLIFY) {
                    assertThat(content[0], is(new String[] {"Feature_Dependencies_Feature",
                        "Feature_Dependencies_Depends_On"}));
                } else {
                    assertThat(content[0], is(new String[] {"Feature", "Depends On"}));
                }
                for (int i = 1; i < content.length; i++) {
                    Assert.assertEquals(2, content[i].length);
                    Assert.assertEquals(expectedDependents[i - 1], content[i][0]);
                    Assert.assertEquals(expectedDependentOns[i - 1], content[i][1]);
                }
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
        File tmpFile = new File(TMP_DIR, "testRelationStructureWithExtraElement.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (SqLiteCollection collection = new SqLiteCollection(tmpFile)) {
            // Write relational data (will create an view)
            try (ITableWriter writer = collection.getWriter("Feature Dependencies")) {
                writer.writeObject(new RelationDataWithExtraElement("A", "B", "Context 1"));
                writer.writeObject(new RelationDataWithExtraElement("A", "C", "Context 2"));
                writer.writeObject(new RelationDataWithExtraElement("B", "C", "Context 3"));
            }
            
            // Test correct creation of view
            String[] expectedDependents = {"A", "A", "B"};
            String[] expectedDependentOns = {"B", "C", "C"};
            String[] expectedContexts = {"Context 1", "Context 2", "Context 3"};
            String tableName = OLD_STYLE_IDENTIFIER_SQLIFY ? "Feature_Dependencies_View" : "Feature Dependencies View";
            try (ITableReader reader = collection.getReader(tableName)) {
                String[][] content = reader.readFull();
                Assert.assertEquals(4, content.length);
                if (OLD_STYLE_IDENTIFIER_SQLIFY) {
                    assertThat(content[0], is(new String[] {"Feature_Dependencies_Feature",
                        "Feature_Dependencies_Depends_On", "Feature_Dependencies_Context"}));
                } else {
                    assertThat(content[0], is(new String[] {"Feature", "Depends On", "Context"}));
                }
                for (int i = 1; i < content.length; i++) {
                    Assert.assertEquals(3, content[i].length);
                    Assert.assertEquals(expectedDependents[i - 1], content[i][0]);
                    Assert.assertEquals(expectedDependentOns[i - 1], content[i][1]);
                    Assert.assertEquals(expectedContexts[i - 1], content[i][2]);
                }
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
        
        File tmpFile = new File(TMP_DIR, "testHeterogeneousStructure.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
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
                Assert.assertEquals(4, firstContent.length);
                if (OLD_STYLE_IDENTIFIER_SQLIFY) {
                    assertThat(firstContent[0], is(new String[] {"Features_name", "Features_value"}));
                } else {
                    assertThat(firstContent[0], is(new String[] {"name", "value"}));
                }
                for (int i = 1; i < firstContent.length; i++) {
                    Assert.assertEquals(firstDataSet.get(i - 1).name, firstContent[i][0]);
                    Assert.assertEquals(firstDataSet.get(i - 1).value, firstContent[i][1]);
                }
            }
            // Contents of relation data (isRelation flag) can be retrieved from table_View
            String tableName = OLD_STYLE_IDENTIFIER_SQLIFY ? "Feature_Dependencies_View" : "Feature Dependencies View";
            try (ITableReader reader = collection.getReader(tableName)) {
                String[][] secondContent = reader.readFull();
                Assert.assertEquals(4, secondContent.length);
                if (OLD_STYLE_IDENTIFIER_SQLIFY) {
                    assertThat(secondContent[0], is(new String[] {"Feature_Dependencies_Feature",
                        "Feature_Dependencies_Depends_On"}));
                } else {
                    assertThat(secondContent[0], is(new String[] {"Feature", "Depends On"}));
                }
                for (int i = 1; i < secondContent.length; i++) {
                    Assert.assertEquals(secondDataSet.get(i - 1).feature, secondContent[i][0]);
                    Assert.assertEquals(secondDataSet.get(i - 1).dependsOn, secondContent[i][1]);
                }
            }
        }
    }
    
    /**
     * Tests that writing before calling {@link ITableWriter#writeHeader(Object...)} throws an {@link IOException}.
     * 
     * @throws IOException wanted.
     */
    @Test(expected = IOException.class)
    public void testWriteBeforeHeader() throws IOException {
        try (SqLiteCollection db = new SqLiteCollection(new File(TMP_DIR, "testWriteBeforeHeader.sqlite"))) {
            try (ITableWriter out = db.getWriter("SomeTable")) {
                out.writeRow("Some", "Row");
            }
        }
    }
    
    /**
     * Tests that writing and reading <code>null</code> values works as expected with
     * {@link SqLiteWriter#writeRow(Object...)}.
     */
    @Test
    public void testWriteAndReadNullRows() {
        File tmpFile = new File(TMP_DIR, "testWriteAndReadNullRows.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        ITableReader reader = null;
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile);
            ITableWriter writer = sqLiteDB.getWriter("Test")) {
            
            writer.writeHeader("First Name", "Last Name");
            writer.writeRow("Donald", "Duck");
            writer.writeRow("Scrooge", null);
            writer.writeRow("Daisy", "Duck");
            
            writer.close();
            
            reader = sqLiteDB.getReader("Test");
            
            assertThat(reader.getLineNumber(), is(0));
            
            if (OLD_STYLE_IDENTIFIER_SQLIFY) {
                assertThat(reader.readNextRow(), is(new String[] {"Test_First_Name", "Test_Last_Name"}));
            } else {
                assertThat(reader.readNextRow(), is(new String[] {"First Name", "Last Name"}));
            }
            assertThat(reader.getLineNumber(), is(0)); // header is not an SQL row
            
            assertThat(reader.readNextRow(), is(new String[] {"Donald", "Duck"}));
            assertThat(reader.getLineNumber(), is(1));
            assertThat(reader.readNextRow(), is(new String[] {"Scrooge", ""}));
            assertThat(reader.getLineNumber(), is(2));
            assertThat(reader.readNextRow(), is(new String[] {"Daisy", "Duck"}));
            assertThat(reader.getLineNumber(), is(3));

            assertThat(reader.readNextRow(), nullValue());
            
        } catch (IOException exc) {
            exc.printStackTrace();
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
     * Tests that writing and reading <code>null</code> values works as expected with
     * {@link SqLiteWriter#writeObject(Object)}.
     */
    @Test
    public void testWriteAndReadNullObject() {
        File tmpFile = new File(TMP_DIR, "testWriteAndReadNullObject.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        ITableReader reader = null;
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile);
            ITableWriter writer = sqLiteDB.getWriter("Test")) {
            
            TestData elem1 = new TestData("element1", "value1");
            TestData elem2 = new TestData("element2", null);
            writer.writeObject(elem1);
            writer.writeObject(elem2);
            writer.close();
            
            reader = sqLiteDB.getReader("Test");
            
            assertContent(reader, "Test", elem1, elem2);
            
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
     * Tests the getFiles() method.
     * 
     * @throws IOException unwanted.
     */
    @Test
    public void testGetFiles() throws IOException {
        File tmpFile = new File(TMP_DIR, "testGetFiles.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile)) {
            Set<@NonNull File> expectedFiles = new HashSet<>();
            expectedFiles.add(tmpFile);
            assertThat(sqLiteDB.getFiles(), is(expectedFiles));
        }
    }
    
    /**
     * Tests the getTableNames() method.
     * 
     * @throws IOException unwanted.
     */
    @Test
    public void testGetTableNames() throws IOException {
        File tmpFile = new File(TMP_DIR, "testGetTableNames.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile)) {
            Set<@NonNull String> expectedTables = new HashSet<>();
            
            assertThat(sqLiteDB.getTableNames(), is(expectedTables));
            
            try (ITableWriter out = sqLiteDB.getWriter("Table 1")) {
                out.writeHeader("Column 1", "Column 2");
            }
            
            expectedTables.add("Table 1");
            assertThat(sqLiteDB.getTableNames(), is(expectedTables));
            
            try (ITableWriter out = sqLiteDB.getWriter("Table 2")) {
                out.writeHeader("Column 1", "Column 2");
            }
            
            expectedTables.add("Table 2");
            assertThat(sqLiteDB.getTableNames(), is(expectedTables));
        }
    }
    
    /**
     * Tests that reading a non-existing table correctly throws an exception.
     * 
     * @throws IOException wanted.
     */
    @Test(expected = IOException.class)
    public void testReadNonExisting() throws IOException {
        File tmpFile = new File(TMP_DIR, "testReadNonExisting.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile)) {
            sqLiteDB.getReader("DoesntExist");
        }
    }
    
    /**
     * Tests that trying to create a writer for an existing table correctly overwrites the table.
     * 
     * @throws IOException unwanted.
     */
    @Test
    public void testWriteExisting() throws IOException {
        File tmpFile = new File(TMP_DIR, "testWriteExisting.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile)) {
            
            try (ITableWriter out = sqLiteDB.getWriter("Table")) {
                out.writeHeader("Column A", "Column B");
                out.writeRow("ABC", "DEF");
            }
            
            try (ITableReader in = sqLiteDB.getReader("Table")) {
                assertThat(in.readNextRow(), is(new String[] {"Column A", "Column B"}));
                assertThat(in.readNextRow(), is(new String[] {"ABC", "DEF"}));
                assertThat(in.readNextRow(), nullValue());
            }
            
            try (ITableWriter out = sqLiteDB.getWriter("Table")) {
                out.writeHeader("Column 1", "Column 2");
                out.writeRow("Alpha", "Beta");
            }
            
            try (ITableReader in = sqLiteDB.getReader("Table")) {
                assertThat(in.readNextRow(), is(new String[] {"Column 1", "Column 2"}));
                assertThat(in.readNextRow(), is(new String[] {"Alpha", "Beta"}));
                assertThat(in.readNextRow(), nullValue());
            }
        }
    }
    
    /**
     * Tests that creating a table with no columns correctly throws an exception.
     * 
     * @throws IOException wanted.
     */
    @Test(expected = IOException.class)
    public void testTableWithNoColumns() throws IOException {
        File tmpFile = new File(TMP_DIR, "testTableWithNoColumns.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile)) {
            
            try (ITableWriter out = sqLiteDB.getWriter("Table")) {
                out.writeHeader();
            }
        }
    }
    
    /**
     * Tests that mixing writeHeader and writeObject correctly throws an exception.
     * 
     * @throws IOException wanted.
     */
    @Test(expected = IOException.class)
    public void testMixWriteObjectAndWriteHeader() throws IOException {
        File tmpFile = new File(TMP_DIR, "testMixWriteObjectAndWriteHeader.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile)) {
            
            try (ITableWriter out = sqLiteDB.getWriter("Table")) {
                out.writeObject(new TestData("A", "B"));
                out.writeHeader("Column1", "Column2");
            }
        }
    }
    
    /**
     * Tests that mixing writeHeader and writeObject correctly throws an exception.
     * 
     * @throws IOException wanted.
     */
    @Test(expected = IOException.class)
    public void testMixWriteHeaderAndWriteObject() throws IOException {
        File tmpFile = new File(TMP_DIR, "testMixWriteHeaderAndWriteObject.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile)) {
            
            try (ITableWriter out = sqLiteDB.getWriter("Table")) {
                out.writeHeader("Column1", "Column2");
                out.writeObject(new TestData("A", "B"));
            }
        }
    }
    
    /**
     * Tests that mixing writeHeader and writeObject correctly throws an exception.
     * 
     * @throws IOException wanted.
     */
    @Test(expected = IOException.class)
    public void testMixWriteRelationalObjectAndWriteHeader() throws IOException {
        File tmpFile = new File(TMP_DIR, "testMixWriteRelationalObjectAndWriteHeader.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile)) {
            
            try (ITableWriter out = sqLiteDB.getWriter("Table")) {
                out.writeObject(new RelationData("A", "B"));
                out.writeHeader("Column1", "Column2");
            }
        }
    }
    
    /**
     * Tests that mixing writeHeader and writeObject correctly throws an exception.
     * 
     * @throws IOException wanted.
     */
    @Test(expected = IOException.class)
    public void testMixWriteHeaderAndWriteRelationalObject() throws IOException {
        File tmpFile = new File(TMP_DIR, "testMixWriteHeaderAndWriteRelationalObject.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqLiteDB = new SqLiteCollection(tmpFile)) {
            
            try (ITableWriter out = sqLiteDB.getWriter("Table")) {
                out.writeHeader("Column1", "Column2");
                out.writeObject(new RelationData("A", "B"));
            }
        }
    }
    
}
