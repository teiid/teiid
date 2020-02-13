/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.infinispan.hotrod;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import javax.resource.ResourceException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.infinispan.api.HotRodTestServer;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Parameter;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.UpdateExecution;

public class TestHotrodExecution {
    private static HotRodTestServer SERVER;
    private static TranslationUtility UTILITY;
    private static RuntimeMetadata METADATA;
    private static InfinispanExecutionFactory EF;
    private static ExecutionContext EC;
    private static int PORT = 11322;
    private static String CACHE_NAME="default";

    @BeforeClass
    public static void setup() throws Exception {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-5"));
        SERVER = new HotRodTestServer(PORT);

        MetadataFactory mf = TestProtobufMetadataProcessor.protoMatadata("tables.proto");
        EF = new InfinispanExecutionFactory();
        EF.setSupportsBulkUpdate(true);
        TransformationMetadata tm = TestProtobufMetadataProcessor.getTransformationMetadata(mf, EF);
        //String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        //System.out.println(ddl);

        METADATA = new RuntimeMetadataImpl(tm);
        UTILITY = new TranslationUtility(tm);


        InfinispanConnection connection = SERVER.getConnection();

        // only use G2 & G4 as cache only support single id
        connection.registerProtobufFile(new ProtobufResource("tables.proto",
        ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tables.proto"))));

        EC = Mockito.mock(ExecutionContext.class);
        Mockito.stub(EC.getBatchSize()).toReturn(512);

    }

    @AfterClass
    public static void tearDown() {
        TimestampWithTimezone.resetCalendar(null);
        SERVER.stop();
    }

    public InfinispanConnection getConnection(String cache) throws ResourceException {
        return SERVER.getConnection(cache);
    }


    @Test
    public void testServer() throws Exception {
        InfinispanConnection connection = getConnection("default");

        CACHE_NAME = connection.getCache().getName();

        ResultSetExecution exec = null;
        Command command = null;
        UpdateExecution update = null;


        // the below also test one-2-one relation.
        command = UTILITY.parseCommand("DELETE FROM G2");
        update = EF.createUpdateExecution(command, EC, METADATA, connection);
        update.execute();

        command = UTILITY.parseCommand("SELECT e1, e2, g3_e1, g3_e2 FROM G2");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertNull(exec.next());

        command = UTILITY.parseCommand("INSERT INTO G2 (e1, e2, g3_e1, g3_e2) values (1, 'one', 1, 'one')");
        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();

        command = UTILITY.parseCommand("INSERT INTO G2 (e1, e2, g3_e1, g3_e2) values (2, 'two', 2, 'two')");
        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();

        command = UTILITY.parseCommand("SELECT e1, e2, g3_e1, g3_e2 FROM G2");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one", new Integer(1), "one"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), "two", new Integer(2), "two"}, exec.next().toArray());
        assertNull(exec.next());

        command = UTILITY.parseCommand("INSERT INTO G4 (e1, e2, G2_e1) values (1, 'one', 1)");
        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();

        command = UTILITY.parseCommand("INSERT INTO G4 (e1, e2, G2_e1) values (2, 'one-one', 1)");
        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();

        command = UTILITY.parseCommand("INSERT INTO G4 (e1, e2, G2_e1) values (3, 'two', 2)");
        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();

        command = UTILITY.parseCommand("INSERT INTO G4 (e1, e2, G2_e1) values (4, 'two-two', 2)");
        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();

        command = UTILITY.parseCommand("SELECT e1, e2 FROM G4");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), "one-one"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(3), "two"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(4), "two-two"}, exec.next().toArray());
        assertNull(exec.next());

        //TEIID-5888
        command = UTILITY.parseCommand("SELECT g2.e1, g4.e1, g4.e2 FROM G2 g2 JOIN G4 g4 "
                + "ON g2.e1 = g4.G2_e1 WHERE g4.e2 = 'two'");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(2), new Integer(3), "two"}, exec.next().toArray());
        assertNull(exec.next());

        command = UTILITY.parseCommand("SELECT g2.e1, g4.e1, g4.e2 FROM G2 g2 JOIN G4 g4 "
                + "ON g2.e1 = g4.G2_e1 WHERE g2.e2 = 'two'");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(2), new Integer(3), "two"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), new Integer(4), "two-two"}, exec.next().toArray());
        assertNull(exec.next());

        // updates
        command = UTILITY.parseCommand("UPDATE G2 SET e2 = 'two-m', g3_e2 = 'two-mm' WHERE e1 = 2");
        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();

        assertArrayEquals(new int[] {2}, update.getUpdateCounts());

        command = UTILITY.parseCommand("SELECT e1, e2, g3_e1, g3_e2 FROM G2 ORDER BY e1");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one", new Integer(1), "one"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), "two-m", new Integer(2), "two-mm"}, exec.next().toArray());
        assertNull(exec.next());

        // complex updates
        command = UTILITY.parseCommand("UPDATE G4 SET e2 = 'two-2' WHERE e2 = 'two-two' OR e2 = 'one-one'");
        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();

        //TEIID-5888
        assertArrayEquals(new int[] {2}, update.getUpdateCounts());

        command = UTILITY.parseCommand("SELECT e1, e2 FROM G4");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), "two-2"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(3), "two"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(4), "two-2"}, exec.next().toArray());
        assertNull(exec.next());

        // deletes
        command = UTILITY.parseCommand("DELETE FROM G4 where e2 = 'two-2'");
        update = EF.createUpdateExecution(command, EC, METADATA, connection);
        update.execute();

        assertArrayEquals(new int[] {2}, update.getUpdateCounts());

        command = UTILITY.parseCommand("SELECT e1, e2 FROM G4");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(3), "two"}, exec.next().toArray());
        assertNull(exec.next());

        command = UTILITY.parseCommand("DELETE FROM G4 where e2 = 'two' AND G2_e1 = 2");
        update = EF.createUpdateExecution(command, EC, METADATA, connection);
        update.execute();

        assertArrayEquals(new int[] {0}, update.getUpdateCounts());

        command = UTILITY.parseCommand("SELECT e1, e2 FROM G4");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one"}, exec.next().toArray());
        assertNull(exec.next());

        // cascade deletes G4
        command = UTILITY.parseCommand("DELETE FROM G2 where e1 = 1");
        update = EF.createUpdateExecution(command, EC, METADATA, connection);
        update.execute();

        assertArrayEquals(new int[] {1}, update.getUpdateCounts());

        command = UTILITY.parseCommand("SELECT * FROM G2");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(2), "two-m", new Integer(2), "two-mm", null, null}, exec.next().toArray());
        assertNull(exec.next());

        // upsert
        command = UTILITY.parseCommand("UPSERT INTO G2 (e1, e2, g3_e1, g3_e2) values (1, 'one', 1, 'one')");
        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();

        assertArrayEquals(new int[] {1}, update.getUpdateCounts());

        command = UTILITY.parseCommand("SELECT * FROM G2 order by e1");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one", new Integer(1), "one", null, null}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), "two-m", new Integer(2), "two-mm", null, null}, exec.next().toArray());
        assertNull(exec.next());

        command = UTILITY.parseCommand("UPSERT INTO G2 (e1, e2, g3_e1, g3_e2) values (2, 'two', 2, 'two')");
        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();

        assertArrayEquals(new int[] {1}, update.getUpdateCounts());

        command = UTILITY.parseCommand("SELECT * FROM G2");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one", new Integer(1), "one", null, null}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), "two", new Integer(2), "two", null, null}, exec.next().toArray());
        assertNull(exec.next());


        command = UTILITY.parseCommand("UPSERT INTO G4 (e1, e2, G2_e1) values (5, 'upsert', 2)");
        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();

        assertArrayEquals(new int[] {1}, update.getUpdateCounts());

        command = UTILITY.parseCommand("SELECT e1, e2 FROM G4");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(5), "upsert"}, exec.next().toArray());
        assertNull(exec.next());

        Timestamp timestamp = new Timestamp(1504889513361L);
        Date date = new Date(1504889513361L);
        Time time = new Time(1504889513361L);

        String sql = "UPSERT INTO G5 (e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18) "
                + "values (512, 'one', convert(512.34, double), 512.25, 256, 1, 't', 1504889513361, convert(123445656.12313, bigdecimal), "
                + "convert(1332434343, biginteger), "
                + "{t '"+new SimpleDateFormat("HH:mm:ss").format(time)+"'}, "
                + "{ts '"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp)+"'}, "
                + "{d '"+new SimpleDateFormat("yyyy-MM-dd").format(date)+"'}, "
                + "null, null, "
                + "convert('clob contents', clob), xmlparse(CONTENT '<a>foo</a>'), null)";

        command = UTILITY.parseCommand(sql);


        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();

        assertArrayEquals(new int[] {1}, update.getUpdateCounts());

        command = UTILITY.parseCommand("SELECT e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18 FROM G5");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        List<?> results = exec.next();
        assertEquals(new Integer(512), results.get(0));
        assertEquals("one", results.get(1));
        //assertEquals(512.34, (double)results.get(2));
        //assertEquals(512.25, (float)results.get(3));
        assertEquals(new Short("256"), results.get(4));
        assertEquals(new Byte("1"), results.get(5));
        assertEquals(new String("t"), results.get(6));
        assertEquals(new Long(1504889513361L), results.get(7));
        assertEquals(new BigDecimal("123445656.12313").toPlainString(), ((BigDecimal)results.get(8)).toPlainString());
        assertEquals(new BigInteger("1332434343").toString(), ((BigInteger)results.get(9)).toString());


        assertEquals(new Time(new SimpleDateFormat("HH:mm:ss").parse(time.toString()).getTime()), results.get(10));
        assertEquals(new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2017-09-08 11:51:53").getTime()), results.get(11));
        assertEquals(new Date(new SimpleDateFormat("yyyy-MM-dd").parse("2017-09-08").getTime()), results.get(12));

        assertEquals("clob contents", ObjectConverterUtil.convertToString(((Clob)results.get(15)).getCharacterStream()));
        assertEquals("<a>foo</a>", ObjectConverterUtil.convertToString(((SQLXML)results.get(16)).getCharacterStream()));
        assertNull(exec.next());

        command = UTILITY.parseCommand("UPSERT INTO G2 (e1) values (3)");
        Insert insert = (Insert)command;
        Parameter param = new Parameter();
        param.setType(Integer.class);
        param.setValueIndex(0);
        ExpressionValueSource evs = new ExpressionValueSource(Arrays.asList((Expression)param));
        insert.setValueSource(evs);
        List<List<?>> vals = new ArrayList<List<?>>();
        for (int i = 0; i < 3; i++) {
            vals.add(Arrays.asList(i));
        }
        insert.setParameterValues(vals.iterator());
        update = EF.createUpdateExecution(command,EC, METADATA, connection);
        update.execute();
        assertTrue(EF.returnsSingleUpdateCount());
        assertArrayEquals(new int[] {3}, update.getUpdateCounts());
    }

    // TEIID-5165 - test large cache delete
    @Test
    public void testServer_Teiid_5165() throws Exception {
        EF.setSupportsUpsert(false);

        ResultSetExecution exec = null;
        Command command = null;
        UpdateExecution update = null;

        InfinispanConnection connection = getConnection("foo");

        // the below also test one-2-one relation.
        command = UTILITY.parseCommand("DELETE FROM G2");
        update = EF.createUpdateExecution(command, EC, METADATA, connection);
        update.execute();

        assertArrayEquals(new int[] {0}, update.getUpdateCounts());

        int rows = 12000;
        for (int i=0; i < rows; i++) {
            command = UTILITY.parseCommand("INSERT INTO G2 (e1, e2, g3_e1, g3_e2) values (" + i +", 'row" + i +"', 1, 'one')");
            update = EF.createUpdateExecution(command,EC, METADATA, connection);
            update.execute();

            assertArrayEquals(new int[] {1}, update.getUpdateCounts());
        }

        Thread.sleep(5000);

        command = UTILITY.parseCommand("SELECT e1, e2 FROM G2");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        int cnt = 0;
        while(true) {
            List<?> results = exec.next();
            if (results == null) break;
            cnt++;
        }

        assertEquals(new Integer(rows), new Integer(cnt));

        command = UTILITY.parseCommand("DELETE FROM G2");
        update = EF.createUpdateExecution(command, EC, METADATA, connection);
        update.execute();

        assertArrayEquals(new int[] {rows}, update.getUpdateCounts());

        command = UTILITY.parseCommand("SELECT count(*) as cnt FROM G2");
        exec = EF.createResultSetExecution((QueryExpression) command, EC, METADATA, connection);
        exec.execute();

        assertNull(exec.next());
    }


}
