package org.teiid.translator.parquet;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.file.JavaVirtualFileConnection;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;

@SuppressWarnings("nls")
public class TestParquetExecution {

    static ArrayList<?> helpExecute(String ddl, VirtualFileConnection connection, String query) throws Exception {
        ParquetExecutionFactory translator = new ParquetExecutionFactory();
        translator.start();

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "vdb", "parquet");
        TranslationUtility utility = new TranslationUtility(metadata);

        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);

        ResultSetExecution execution = translator.createResultSetExecution((QueryExpression)cmd, context, utility.createRuntimeMetadata(), connection);
        try {
            execution.execute();

            ArrayList<Object> results = new ArrayList<>();
            while (true) {
                List<?> row = execution.next();
                if (row == null) {
                    break;
                }
                results.add(row);
            }
            return results;
        } finally {
            execution.close();
        }
    }

    @Test
    public void testParquetExecutionWithListAsAColumn() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "	contacts integer[] ,\n" +
                "	id long ,\n" +
                "	last string ,\n" +
                "	name string ,\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select * from Table1");
        Assert.assertEquals("[[[21232, 98989, 9898999], 1, Phelps, Michael], [[21999, 98909, 809809], 2, Marie, Anne]]", results.toString());
    }

    @Test
    public void testParquetExecution() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "	firstname string ,\n" +
                "	id long ,\n" +
                "	lastname string ,\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select * from Table1");
        Assert.assertEquals("[[Aditya, 1, Sharma], [Animesh, 2, Sharma], [Shradha, 3, Khapra]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithDirectoryBasedPartitioning() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "	id long ,\n" +
                "	\"month\" string ,\n" +
                "	name string ,\n" +
                "	\"year\" long ,\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'dir', \"teiid_parquet:PARTITIONED_COLUMNS\" 'year,month');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select name from Table1 WHERE \"year\"=2019 and \"month\"='January'");
        Assert.assertEquals("[[Michael], [Anne]]",results.toString());
    }

    @Test
    public void testParquetExecutionWithPathFilter() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   id long ,\n" +
                "   \"month\" string ,\n" +
                "   name string ,\n" +
                "   \"year\" long ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'dir', \"teiid_parquet:PARTITIONED_COLUMNS\" 'year,month');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select name, \"month\" from Table1 WHERE \"month\">'January'");
        Assert.assertEquals("[[Michael, March], [Anne, March]]",results.toString());

        results = helpExecute(ddl, connection, "select name, \"year\" from Table1 WHERE \"year\"<2020");
        Assert.assertEquals("[[Michael, 2019], [Anne, 2019], [Michael, 2019], [Anne, 2019]]",results.toString());

        results = helpExecute(ddl, connection, "select name, \"year\" from Table1 WHERE \"year\">2021");
        Assert.assertEquals("[]",results.toString());
    }

    @Test
    public void testParquetExecutionWithNestedPartitionFilter() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   id long ,\n" +
                "   \"month\" string ,\n" +
                "   name string ,\n" +
                "   \"year\" long ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'dir', \"teiid_parquet:PARTITIONED_COLUMNS\" 'year,month');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        //nested partition predicate
        ArrayList<?> results = helpExecute(ddl, connection, "select name, \"month\" from Table1 WHERE \"month\"='January' or name='Michael'");

        //this reads over multiple files, so compare without dictating the order
        Assert.assertEquals(
                new HashSet<List<String>>(
                        Arrays.asList(Arrays.asList("Michael", "January"),
                                Arrays.asList("Michael", "March"),
                                Arrays.asList("Anne", "January"))),
                new HashSet<>(results));
    }

    @Test
    public void testParquetExecutionWithProjectedColumns() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "	firstname string ,\n" +
                "	id long ,\n" +
                "	lastname string ,\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select firstname, lastname from Table1");
        Assert.assertEquals("[[Aditya, Sharma], [Animesh, Sharma], [Shradha, Khapra]]", results.toString());
    }

    @Test
    public void testParquetExecutionNonProjectedPredicate() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   firstname string ,\n" +
                "   id long ,\n" +
                "   lastname string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select firstname, lastname from Table1 where id = 1");
        Assert.assertEquals("[[Aditya, Sharma]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithRowGroupFilter() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "	firstname string ,\n" +
                "	id integer ,\n" +
                "	lastname string ,\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select firstname, lastname from Table1 WHERE firstname='Aditya'");
        Assert.assertEquals("[[Aditya, Sharma]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithDirectoryBasedPartitioningAndRowFilter() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "	id integer ,\n" +
                "	\"month\" string ,\n" +
                "	name string ,\n" +
                "	\"year\" long ,\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'dir', \"teiid_parquet:PARTITIONED_COLUMNS\" 'year,month');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select name from Table1 WHERE \"year\"=2019 and \"month\"='January' and name='Michael'");
        Assert.assertEquals("[[Michael]]",results.toString());
    }

    @Test
    public void testParquetExecutionWithDirectoryBasedPartitioningAndPartitionedColumnSelect() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "	id integer ,\n" +
                "	\"month\" string ,\n" +
                "	name string ,\n" +
                "	\"year\" long ,\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'dir', \"teiid_parquet:PARTITIONED_COLUMNS\" 'year,month');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select name, \"year\" from Table1 WHERE \"year\"=2019 and \"month\"='January'");
        Assert.assertEquals("[[Michael, 2019], [Anne, 2019]]",results.toString());
    }

    @Test
    public void testParquetExecutionWithRowFilterEq() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   firstname string ,\n" +
                "   id long ,\n" +
                "   lastname string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select id,firstname from Table1 WHERE id=2");
        Assert.assertEquals("[[2, Animesh]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithNePredicate() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   contacts integer[] ,\n" +
                "   id long ,\n" +
                "   last string ,\n" +
                "   name string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select * from Table1 WHERE id<>2");
        assertEquals(1, results.size());
    }

    /*
     * It is possible to resolve some type conflicts like this automatically, but for now
     * we'll just error out
     */
    @Test(expected = IllegalArgumentException.class)
    public void testParquetExecutionWithInvalidType() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   contacts integer[] ,\n" +
                "   id integer ,\n" + //id is actually a long in the schema
                "   last string ,\n" +
                "   name string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        helpExecute(ddl, connection, "select * from Table1 WHERE id=1");
    }

    @Test
    public void testParquetExecutionWithRowFilterGt() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   firstname string ,\n" +
                "   id long ,\n" +
                "   lastname string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select id,firstname from Table1 WHERE id>2");
        Assert.assertEquals("[[3, Shradha]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithRowFilterLt() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   firstname string ,\n" +
                "   id long ,\n" +
                "   lastname string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select id,firstname from Table1 WHERE id<3");
        Assert.assertEquals("[[1, Aditya], [2, Animesh]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithRowFilterGtEq() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   firstname string ,\n" +
                "   id long ,\n" +
                "   lastname string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select id,firstname from Table1 WHERE id>=2");
        Assert.assertEquals("[[2, Animesh], [3, Shradha]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithRowFilterLtEq() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   firstname string ,\n" +
                "   id long ,\n" +
                "   lastname string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select id,firstname from Table1 WHERE id<=2");
        Assert.assertEquals("[[1, Aditya], [2, Animesh]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithRowFilterLtNotEq() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   firstname string ,\n" +
                "   id long ,\n" +
                "   lastname string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select id,firstname from Table1 WHERE id!=2");
        Assert.assertEquals("[[1, Aditya], [3, Shradha]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithRowFilterWithMultiplePredicatesOnSameColumn() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   firstname string ,\n" +
                "   id long ,\n" +
                "   lastname string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select id,firstname from Table1 WHERE id>1 and id<3");
        Assert.assertEquals("[[2, Animesh]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithRowFilterIsNull() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   firstname string ,\n" +
                "   id long ,\n" +
                "   lastname string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select id,firstname from Table1 WHERE firstName is null");
        Assert.assertEquals("[]", results.toString());
    }

    @Test
    public void testParquetExecutionWithRowFilterIsNotNull() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   firstname string ,\n" +
                "   id long ,\n" +
                "   lastname string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select id,firstname from Table1 WHERE firstName is not null");
        Assert.assertEquals("[[1, Aditya], [2, Animesh], [3, Shradha]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithRowFilterOr() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "   firstname string ,\n" +
                "   id long ,\n" +
                "   lastname string ,\n" +
                "   CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:LOCATION\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList<?> results = helpExecute(ddl, connection, "select id,firstname from Table1 WHERE firstName = 'Aditya' or id = 2");
        Assert.assertEquals("[[1, Aditya], [2, Animesh]]", results.toString());
    }

}
