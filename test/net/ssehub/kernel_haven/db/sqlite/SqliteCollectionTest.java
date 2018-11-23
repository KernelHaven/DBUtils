package net.ssehub.kernel_haven.db.sqlite;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
 * Tests the {@link SqliteCollection}.
 * 
 * @author Adam
 * @author El-Sharkawy
 */
@SuppressWarnings("null")
public class SqliteCollectionTest {
    
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
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile);
            ITableWriter writer = sqliteDB.getWriter("Test")) {
            
            TestData elem1 = new TestData("element1", "value1");
            TestData elem2 = new TestData("element2", "value2");
            writer.writeObject(elem1);
            writer.writeObject(elem2);
            writer.close();
            
            reader = sqliteDB.getReader("Test");
            
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
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile);
            ITableWriter writer = sqliteDB.getWriter("Test")) {
            
            writer.writeHeader("First Name", "Last Name");
            writer.writeRow("Donald", "Duck");
            writer.writeRow("Scrooge", "McDuck");
            writer.writeRow("Daisy", "Duck");
            
            writer.close();
            
            reader = sqliteDB.getReader("Test");
            
            assertThat(reader.getLineNumber(), is(0));
            
            assertThat(reader.readNextRow(), is(new String[] {"First Name", "Last Name"}));
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
        
        ITableReader reader = null;
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile);
            ITableWriter writer = sqliteDB.getWriter("Test Table")) {
            
            TestData elem1 = new TestData("element1", "value1");
            TestData elem2 = new TestData("element2", "value2");
            writer.writeObject(elem1);
            writer.writeObject(elem2);
            writer.close();
            
            reader = sqliteDB.getReader("Test Table");
            
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
        assertThat(reader.readNextRow(), is(new String[] {"name", "value"}));
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
        
        try (SqliteCollection collection = new SqliteCollection(tmpFile)) {
            // Write relational data (will create an view)
            try (ITableWriter writer = collection.getWriter("Feature Dependencies")) {
                writer.writeObject(new RelationData("A", "B"));
                writer.writeObject(new RelationData("A", "C"));
                writer.writeObject(new RelationData("B", "C"));
            }
            
            // Test correct creation of view
            String[] expectedDependents = {"A", "A", "B"};
            String[] expectedDependentOns = {"B", "C", "C"};
            
            try (ITableReader reader = collection.getReader("Feature Dependencies View")) {
                String[][] content = reader.readFull();
                Assert.assertEquals(4, content.length);
                assertThat(content[0], is(new String[] {"Feature", "Depends On"}));
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
        
        try (SqliteCollection collection = new SqliteCollection(tmpFile)) {
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
            try (ITableReader reader = collection.getReader("Feature Dependencies View")) {
                String[][] content = reader.readFull();
                Assert.assertEquals(4, content.length);
                assertThat(content[0], is(new String[] {"Feature", "Depends On", "Context"}));
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
        try (SqliteCollection collection = new SqliteCollection(tmpFile)) {
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
        try (SqliteCollection collection = new SqliteCollection(tmpFile)) {
            try (ITableReader reader = collection.getReader("Features")) {
                String[][] firstContent = reader.readFull();
                Assert.assertEquals(4, firstContent.length);
                assertThat(firstContent[0], is(new String[] {"name", "value"}));
                for (int i = 1; i < firstContent.length; i++) {
                    Assert.assertEquals(firstDataSet.get(i - 1).name, firstContent[i][0]);
                    Assert.assertEquals(firstDataSet.get(i - 1).value, firstContent[i][1]);
                }
            }
            // Contents of relation data (isRelation flag) can be retrieved from table_View
            try (ITableReader reader = collection.getReader("Feature Dependencies View")) {
                String[][] secondContent = reader.readFull();
                Assert.assertEquals(4, secondContent.length);
                assertThat(secondContent[0], is(new String[] {"Feature", "Depends On"}));
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
        try (SqliteCollection db = new SqliteCollection(new File(TMP_DIR, "testWriteBeforeHeader.sqlite"))) {
            try (ITableWriter out = db.getWriter("SomeTable")) {
                out.writeRow("Some", "Row");
            }
        }
    }
    
    /**
     * Tests that writing and reading <code>null</code> values works as expected with
     * {@link SqliteWriter#writeRow(Object...)}.
     */
    @Test
    public void testWriteAndReadNullRows() {
        File tmpFile = new File(TMP_DIR, "testWriteAndReadNullRows.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        ITableReader reader = null;
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile);
            ITableWriter writer = sqliteDB.getWriter("Test")) {
            
            writer.writeHeader("First Name", "Last Name");
            writer.writeRow("Donald", "Duck");
            writer.writeRow("Scrooge", null);
            writer.writeRow("Daisy", "Duck");
            
            writer.close();
            
            reader = sqliteDB.getReader("Test");
            
            assertThat(reader.getLineNumber(), is(0));
            
            assertThat(reader.readNextRow(), is(new String[] {"First Name", "Last Name"}));
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
     * {@link SqliteWriter#writeObject(Object)}.
     */
    @Test
    public void testWriteAndReadNullObject() {
        File tmpFile = new File(TMP_DIR, "testWriteAndReadNullObject.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        ITableReader reader = null;
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile);
            ITableWriter writer = sqliteDB.getWriter("Test")) {
            
            TestData elem1 = new TestData("element1", "value1");
            TestData elem2 = new TestData("element2", null);
            writer.writeObject(elem1);
            writer.writeObject(elem2);
            writer.close();
            
            reader = sqliteDB.getReader("Test");
            
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
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            Set<@NonNull File> expectedFiles = new HashSet<>();
            expectedFiles.add(tmpFile);
            assertThat(sqliteDB.getFiles(), is(expectedFiles));
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
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            Set<@NonNull String> expectedTables = new HashSet<>();
            
            assertThat(sqliteDB.getTableNames(), is(expectedTables));
            
            try (ITableWriter out = sqliteDB.getWriter("Table 1")) {
                out.writeHeader("Column 1", "Column 2");
            }
            
            expectedTables.add("Table 1");
            assertThat(sqliteDB.getTableNames(), is(expectedTables));
            
            try (ITableWriter out = sqliteDB.getWriter("Table 2")) {
                out.writeHeader("Column 1", "Column 2");
            }
            
            expectedTables.add("Table 2");
            assertThat(sqliteDB.getTableNames(), is(expectedTables));
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
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            sqliteDB.getReader("DoesntExist");
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
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            
            try (ITableWriter out = sqliteDB.getWriter("Table")) {
                out.writeHeader("Column A", "Column B");
                out.writeRow("ABC", "DEF");
            }
            
            try (ITableReader in = sqliteDB.getReader("Table")) {
                assertThat(in.readNextRow(), is(new String[] {"Column A", "Column B"}));
                assertThat(in.readNextRow(), is(new String[] {"ABC", "DEF"}));
                assertThat(in.readNextRow(), nullValue());
            }
            
            try (ITableWriter out = sqliteDB.getWriter("Table")) {
                out.writeHeader("Column 1", "Column 2");
                out.writeRow("Alpha", "Beta");
            }
            
            try (ITableReader in = sqliteDB.getReader("Table")) {
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
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            
            try (ITableWriter out = sqliteDB.getWriter("Table")) {
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
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            
            try (ITableWriter out = sqliteDB.getWriter("Table")) {
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
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            
            try (ITableWriter out = sqliteDB.getWriter("Table")) {
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
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            
            try (ITableWriter out = sqliteDB.getWriter("Table")) {
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
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            
            try (ITableWriter out = sqliteDB.getWriter("Table")) {
                out.writeHeader("Column1", "Column2");
                out.writeObject(new RelationData("A", "B"));
            }
        }
    }
    
    /**
     * Tests writing a header with a null value.
     * 
     * @throws IOException wanted.
     */
    @Test(expected = IOException.class)
    public void testWriteNullHeader() throws IOException {
        File tmpFile = new File(TMP_DIR, "testWriteNullHeader.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            
            try (ITableWriter out = sqliteDB.getWriter("Table")) {
                out.writeHeader("Column1", null, "Column3");
            }
        }
    }
    
    /**
     * Tests that trying to write a too long row correctly throws an exception.
     * 
     * @throws IOException wanted.
     */
    @Test(expected = IOException.class)
    public void testWriteTooLargeRow() throws IOException {
        File tmpFile = new File(TMP_DIR, "testWriteTooLargeRow.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            
            try (ITableWriter out = sqliteDB.getWriter("Table")) {
                out.writeHeader("Column1", "Column2", "Column3");
                out.writeRow("Value1", "Value2", "Value3", "Value4");
            }
        }
    }
    
    /**
     * Tests that trying to write a too short row correctly throws an exception.
     * 
     * @throws IOException wanted.
     */
    @Test(expected = IOException.class)
    public void testWriteTooSmallRow() throws IOException {
        File tmpFile = new File(TMP_DIR, "testWriteTooSmallRow.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            
            try (ITableWriter out = sqliteDB.getWriter("Table")) {
                out.writeHeader("Column1", "Column2", "Column3");
                out.writeRow("Value1", "Value2");
            }
        }
    }
    
    /**
     * Tests that names with quotation marks (") are handled correctly.
     * 
     * @throws IOException unwanted.
     */
    @Test
    public void testNamesWithQuotationMark() throws IOException {
        File tmpFile = new File(TMP_DIR, "testNamesWithQuotationMark.sqlite");
        assertThat(tmpFile.exists(), is(false));
        
        try (ITableCollection sqliteDB = new SqliteCollection(tmpFile)) {
            
            assertThat(sqliteDB.getTableNames(), is(new HashSet<>()));
            
            try (ITableWriter out = sqliteDB.getWriter("Some\"Table")) {
                out.writeHeader("Column\"A", "Column\"B");
                out.writeRow("A\"B", "C\"D");
            }
            
            assertThat(sqliteDB.getTableNames(), is(new HashSet<>(Arrays.asList("Some\"Table"))));
            
            try (ITableReader in = sqliteDB.getReader("Some\"Table")) {
                assertThat(in.readNextRow(), is(new String[] {"Column\"A", "Column\"B"}));
                assertThat(in.readNextRow(), is(new String[] {"A\"B", "C\"D"}));
                assertThat(in.readNextRow(), nullValue());
            }
        }
    }
    
    /**
     * Tests reading an existing database.
     * 
     * @throws IOException unwanted.
     */
    @Test
    public void testReadExisting() throws IOException {
        File file = new File(AllTests.TESTDATA, "existing.sqlite");
        
        try (SqliteCollection sqliteDb = new SqliteCollection(file)) {
            
            assertThat(sqliteDb.getFiles(), is(new HashSet<>(Arrays.asList(file))));
            assertThat(sqliteDb.getTableNames(), is(new HashSet<>(Arrays.asList("Table No ID", "Table With ID"))));
            
            try (ITableReader in = sqliteDb.getReader("Table No ID")) {
                assertThat(in.readNextRow(), is(new String[] {"Name", "Value"}));
                assertThat(in.readNextRow(), is(new String[] {"A", "1"}));
                assertThat(in.readNextRow(), is(new String[] {"B", "2"}));
                assertThat(in.readNextRow(), is(new String[] {"C", "3"}));
                assertThat(in.readNextRow(), nullValue());
            }
            
            try (ITableReader in = sqliteDb.getReader("Table With ID")) {
                assertThat(in.readNextRow(), is(new String[] {"Value", "Square"}));
                assertThat(in.readNextRow(), is(new String[] {"1", "1"}));
                assertThat(in.readNextRow(), is(new String[] {"2", "4"}));
                assertThat(in.readNextRow(), is(new String[] {"3", "9"}));
                assertThat(in.readNextRow(), nullValue());
            }
        }
    }
    
    /**
     * Tests reading an existing file that is not a Sqlite DB.
     * 
     * @throws IOException wanted.
     */
    @Test(expected = IOException.class)
    public void testReadCorrupted() throws IOException {
        File file = new File(AllTests.TESTDATA, "corrupted.sqlite");
        new SqliteCollection(file).close();
    }

}