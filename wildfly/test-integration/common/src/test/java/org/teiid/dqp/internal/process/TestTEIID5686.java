package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestTEIID5686 {

    private EmbeddedServer es;

    @After
    public void after() {
        es.stop();
    }

    @Test
    public void testJoinPlanInProcedureLoop() throws Exception  {
        es = new EmbeddedServer();
        es.start(new EmbeddedConfiguration());

        HardCodedExecutionFactory ef = new HardCodedExecutionFactory() {
            @Override
            public Object getConnection(Object factory,
                    ExecutionContext executionContext)
                    throws TranslatorException {
                return null;
            }
        };
        ef.setSupportsInnerJoins(true);

        List<List<? extends Object>> e = Arrays.asList(
                Arrays.asList ("test", 1234),
                Arrays.asList ("abcde", 1111),
                Arrays.asList ("abtest", 2222),
                Arrays.asList ("testab", 3333),
                Arrays.asList ("abtestab", 4444));

        ef.addData("SELECT test_e.e FROM test_e", e.stream().map(l->l.subList(0, 1)).collect(Collectors.toList()));

        List<List<Integer>> a = Arrays.asList(
                Arrays.asList (1, 1),
                Arrays.asList (1, 2),
                Arrays.asList (2, 1),
                Arrays.asList (2, 2),
                Arrays.asList (3, 2),
                Arrays.asList (3, 10));
        ef.addData("SELECT test_a.a, test_a.b FROM test_a", a);

        ef.addData(
                "SELECT test_e.e, test_e.f, test_a.a, test_a.b FROM test_e, test_a",
                e.stream().flatMap(l -> a.stream().map(a_row -> {
                    ArrayList<Object> row = new ArrayList<>();
                    row.addAll(l);
                    row.addAll(a_row);
                    return row;
                })).collect(Collectors.toList()));

        es.addTranslator("pg", ef);

        es.addConnectionFactory("pg", Mockito.mock(DataSource.class));

        ModelMetaData physical = new ModelMetaData();
        physical.setName("test_tables_pg");

        physical.addSourceMetadata("ddl", "\n" +
                "CREATE foreign TABLE test_a\n" +
                "(\n" +
                "  a integer,\n" +
                "  b integer\n" +
                ") options (cardinality 5);\nCREATE foreign TABLE test_e\n" +
                "(\n" +
                "  e varchar(254),\n" +
                "  f integer\n" +
                ") options (cardinality 6);");
        physical.addSourceMapping("physical", "pg", "pg");

        ModelMetaData virtual = new ModelMetaData();
        virtual.setModelType(Type.VIRTUAL);
        virtual.setName("views");
        virtual.addSourceMetadata("ddl", "create view v as\n" +
                "        select *\n" +
                "        from(\n" +
                "            select e\n" +
                "            from \"test_tables_pg.test_e\" tb1, TEXTTABLE(tb1.e COLUMNS col1 string) x\n" +
                "        ) t \n" +
                "        join \"test_tables_pg.test_a\" tb2 \n" +
                "        on true");

        es.deployVDB("vdb", physical, virtual);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);

        Statement s = c.createStatement();
        s.executeQuery("begin\n" +
                "    declare integer i=0;\n" +
                "    while (i < 2)\n" +
                "        begin\n" +
                "            select * \n" +
                "            from \"test_tables_pg.test_a\" t0\n" +
                "            JOIN \"test_tables_pg.test_e\" t1\n" +
                "              ON true\n" +
                "            JOIN \"test_tables_pg.test_e\" t2\n" +
                "              ON true\n" +
                "            JOIN views.v t3\n" +
                "                  on true\n" +
                "            JOIN views.v t4\n" +
                "                  on true\n" +
                "            limit 257 ;\n" +
                "            i=i+1;\n" +
                "        end\n" +
                "end");

        ResultSet rs = s.getResultSet();
        int count = 0;
        while (rs.next()) {
            count++;
        }
        s.close();
        assertEquals(257, count);
    }
}
