package org.teiid.translator.parquet;

import java.util.ArrayList;
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

public class TestParquetExecution {

    static ArrayList helpExecute(String ddl, VirtualFileConnection connection, String query) throws Exception {
        ParquetExecutionFactory translator = new ParquetExecutionFactory();
        translator.start();

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "vdb", "parquet");
        TranslationUtility utility = new TranslationUtility(metadata);

        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);

        ResultSetExecution execution = translator.createResultSetExecution((QueryExpression)cmd, context, utility.createRuntimeMetadata(), connection);
        try {
            execution.execute();

            ArrayList results = new ArrayList();
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
                "	id integer ,\n" +
                "	last string ,\n" +
                "	name string ,\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:FILE\" 'people.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList results = helpExecute(ddl, connection, "select * from Table1");
        Assert.assertEquals("[[[21232, 98989, 9898999], 1, Phelps, Michael], [[21999, 98909, 809809], 2, Marie, Anne]]", results.toString());
    }

    @Test
    public void testParquetExecution() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "	firstname string ,\n" +
                "	id integer ,\n" +
                "	lastname string ,\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:FILE\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList results = helpExecute(ddl, connection, "select * from Table1");
        Assert.assertEquals("[[Aditya, 1, Sharma], [Animesh, 2, Sharma], [Shradha, 3, Khapra]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithDirectoryBasedPartitioning() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "	id integer ,\n" +
                "	\"month\" string ,\n" +
                "	name string ,\n" +
                "	\"year\" long ,\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:FILE\" 'dir', \"teiid_parquet:PARTITIONED_COLUMNS\" 'year,month');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList results = helpExecute(ddl, connection, "select name from Table1 WHERE \"year\"=2019 and \"month\"='January'");
        Assert.assertEquals("[[Michael], [Anne]]",results.toString());
    }

    @Test
    public void testParquetExecutionWithProjectedColumns() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "	firstname string ,\n" +
                "	id integer ,\n" +
                "	lastname string ,\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:FILE\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList results = helpExecute(ddl, connection, "select firstname, lastname from Table1");
        Assert.assertEquals("[[Aditya, Sharma], [Animesh, Sharma], [Shradha, Khapra]]", results.toString());
    }

    @Test
    public void testParquetExecutionWithRowGroupFilter() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Table1 (\n" +
                "	firstname string ,\n" +
                "	id integer ,\n" +
                "	lastname string ,\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(id)\n" +
                ") OPTIONS (\"teiid_parquet:FILE\" 'people1.parquet');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList results = helpExecute(ddl, connection, "select firstname, lastname from Table1 WHERE firstname='Aditya'");
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
                ") OPTIONS (\"teiid_parquet:FILE\" 'dir', \"teiid_parquet:PARTITIONED_COLUMNS\" 'year,month');";

        VirtualFileConnection connection = new JavaVirtualFileConnection(UnitTestUtil.getTestDataPath());

        ArrayList results = helpExecute(ddl, connection, "select name from Table1 WHERE \"year\"=2019 and \"month\"='January' and name='Michael'");
        Assert.assertEquals("[[Michael]]",results.toString());
    }

}
