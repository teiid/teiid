/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.infinispan.hotrod;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

import javax.resource.ResourceException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.infinispan.api.HotRodTestServer;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.resource.adapter.infinispan.hotrod.InfinispanConnectionImpl;
import org.teiid.resource.adapter.infinispan.hotrod.InfinispanManagedConnectionFactory;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.UpdateExecution;

/*
 * TODO:
 * HotRodServer is still missing some configuration I could not figure out easily to turn on for testcase.
 * I tested with local running server.
 */
@Ignore
public class TestHotrodExecution {
    private static HotRodTestServer server;
    private static RuntimeMetadata metadata;
    private static InfinispanExecutionFactory ef;
    private static TranslationUtility utility;

    private BasicConnectionFactory<InfinispanConnectionImpl> connectionFactory;

    @BeforeClass
    public static void setup() throws Exception {
        //server = new HotRodTestServer(31323);
        MetadataFactory mf = TestProtobufMetadataProcessor.protoMatadata("tables.proto");
        ef = new InfinispanExecutionFactory();
        TransformationMetadata tm = TestProtobufMetadataProcessor.getTransformationMetadata(mf, ef);
        //String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        //System.out.println(ddl);
        
        metadata = new RuntimeMetadataImpl(tm);
        utility = new TranslationUtility(tm);
    }

    @AfterClass
    public static void tearDown() {
        //server.stop();
    }

    public InfinispanConnection getConnection() throws ResourceException {
        if (connectionFactory == null) {
            InfinispanManagedConnectionFactory factory = new InfinispanManagedConnectionFactory();
            factory.setCacheName("default");
            factory.setRemoteServerList("127.0.0.1:11322");
            connectionFactory = factory.createConnectionFactory();
        }
        return this.connectionFactory.getConnection();
    }


    @Test
    public void testServer() throws Exception {
        InfinispanConnection connection = getConnection();
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        Mockito.stub(ec.getBatchSize()).toReturn(512);

        ResultSetExecution exec = null;
        Command command = null;
        UpdateExecution update = null;

        // only use G2 & G4 as cache only support single id
        connection.registerProtobufFile(new ProtobufResource("tables.proto",
        ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tables.proto"))));

        // the below also test one-2-one relation.
        command = utility.parseCommand("DELETE FROM G2");
        update = ef.createUpdateExecution(command, ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("SELECT e1, e2, g3_e1, g3_e2 FROM G2");
        exec = ef.createResultSetExecution((QueryExpression) command, ec, metadata, connection);
        exec.execute();

        assertNull(exec.next());

        command = utility.parseCommand("INSERT INTO G2 (e1, e2, g3_e1, g3_e2) values (1, 'one', 1, 'one')");
        update = ef.createUpdateExecution(command,ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("INSERT INTO G2 (e1, e2, g3_e1, g3_e2) values (2, 'two', 2, 'two')");
        update = ef.createUpdateExecution(command,ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("SELECT e1, e2, g3_e1, g3_e2 FROM G2");
        exec = ef.createResultSetExecution((QueryExpression) command, ec, metadata, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one", new Integer(1), "one"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), "two", new Integer(2), "two"}, exec.next().toArray());
        assertNull(exec.next());

        command = utility.parseCommand("INSERT INTO G4 (e1, e2, G2_e1) values (1, 'one', 1)");
        update = ef.createUpdateExecution(command,ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("INSERT INTO G4 (e1, e2, G2_e1) values (2, 'one-one', 1)");
        update = ef.createUpdateExecution(command,ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("INSERT INTO G4 (e1, e2, G2_e1) values (3, 'two', 2)");
        update = ef.createUpdateExecution(command,ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("INSERT INTO G4 (e1, e2, G2_e1) values (4, 'two-two', 2)");
        update = ef.createUpdateExecution(command,ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("SELECT e1, e2 FROM G4");
        exec = ef.createResultSetExecution((QueryExpression) command, ec, metadata, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), "one-one"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(3), "two"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(4), "two-two"}, exec.next().toArray());
        assertNull(exec.next());

        command = utility.parseCommand("SELECT g2.e1, g4.e1, g4.e2 FROM G2 g2 JOIN G4 g4 "
                + "ON g2.e1 = g4.G2_e1 WHERE g2.e2 = 'two'");
        exec = ef.createResultSetExecution((QueryExpression) command, ec, metadata, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(2), new Integer(3), "two"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), new Integer(4), "two-two"}, exec.next().toArray());
        assertNull(exec.next());

        // updates
        command = utility.parseCommand("UPDATE G2 SET e2 = 'two-m', g3_e2 = 'two-mm' WHERE e1 = 2");
        update = ef.createUpdateExecution(command,ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("SELECT e1, e2, g3_e1, g3_e2 FROM G2 ORDER BY e1");
        exec = ef.createResultSetExecution((QueryExpression) command, ec, metadata, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one", new Integer(1), "one"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), "two-m", new Integer(2), "two-mm"}, exec.next().toArray());
        assertNull(exec.next());

        // complex updates
        command = utility.parseCommand("UPDATE G4 SET e2 = 'two-2' WHERE e2 = 'two-two' OR e2 = 'one-one'");
        update = ef.createUpdateExecution(command,ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("SELECT e1, e2 FROM G4");
        exec = ef.createResultSetExecution((QueryExpression) command, ec, metadata, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), "two-2"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(3), "two"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(4), "two-2"}, exec.next().toArray());
        assertNull(exec.next());

        // deletes
        command = utility.parseCommand("DELETE FROM G4 where e2 = 'two-2'");
        update = ef.createUpdateExecution(command, ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("SELECT e1, e2 FROM G4");
        exec = ef.createResultSetExecution((QueryExpression) command, ec, metadata, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one"}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(3), "two"}, exec.next().toArray());
        assertNull(exec.next());

        command = utility.parseCommand("DELETE FROM G2 where e1 = 1");
        update = ef.createUpdateExecution(command, ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("SELECT * FROM G2");
        exec = ef.createResultSetExecution((QueryExpression) command, ec, metadata, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(2), "two-m", new Integer(2), "two-mm", null, null}, exec.next().toArray());
        assertNull(exec.next());

        // upsert
        command = utility.parseCommand("UPSERT INTO G2 (e1, e2, g3_e1, g3_e2) values (1, 'one', 1, 'one')");
        update = ef.createUpdateExecution(command,ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("SELECT * FROM G2");
        exec = ef.createResultSetExecution((QueryExpression) command, ec, metadata, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one", new Integer(1), "one", null, null}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), "two-m", new Integer(2), "two-mm", null, null}, exec.next().toArray());
        assertNull(exec.next());

        command = utility.parseCommand("UPSERT INTO G2 (e1, e2, g3_e1, g3_e2) values (2, 'two', 2, 'two')");
        update = ef.createUpdateExecution(command,ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("SELECT * FROM G2");
        exec = ef.createResultSetExecution((QueryExpression) command, ec, metadata, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(1), "one", new Integer(1), "one", null, null}, exec.next().toArray());
        assertArrayEquals(new Object[] {new Integer(2), "two", new Integer(2), "two", null, null}, exec.next().toArray());
        assertNull(exec.next());


        command = utility.parseCommand("UPSERT INTO G4 (e1, e2, G2_e1) values (5, 'upsert', 2)");
        update = ef.createUpdateExecution(command,ec, metadata, connection);
        update.execute();

        command = utility.parseCommand("SELECT e1, e2 FROM G4");
        exec = ef.createResultSetExecution((QueryExpression) command, ec, metadata, connection);
        exec.execute();

        assertArrayEquals(new Object[] {new Integer(3), "two"}, exec.next().toArray());
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
        System.out.println(sql);
        command = utility.parseCommand(sql);
        
        
        update = ef.createUpdateExecution(command,ec, metadata, connection);
        update.execute();        
        
        command = utility.parseCommand("SELECT e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18 FROM G5");
        exec = ef.createResultSetExecution((QueryExpression) command, ec, metadata, connection);
        exec.execute();

        List<?> results = exec.next();
        assertEquals(new Integer(512), (Integer)results.get(0));
        assertEquals("one", (String)results.get(1));
        //assertEquals(512.34, (double)results.get(2));
        //assertEquals(512.25, (float)results.get(3));
        assertEquals(new Short("256"), results.get(4));
        assertEquals(new Byte("1"), results.get(5));
        assertEquals(new String("t"), results.get(6));
        assertEquals(new Long(1504889513361L), results.get(7));
        assertEquals(new BigDecimal("123445656.12313").toPlainString(), ((BigDecimal)results.get(8)).toPlainString());
        assertEquals(new BigInteger("1332434343").toString(), ((BigInteger)results.get(9)).toString());
        
        
        assertEquals(new Time(new SimpleDateFormat("HH:mm:ss").parse("11:51:53").getTime()), results.get(10));
        assertEquals(new Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2017-09-08 11:51:53").getTime()), results.get(11));
        assertEquals(new Date(new SimpleDateFormat("yyyy-MM-dd").parse("2017-09-08").getTime()), results.get(12));
        
        assertEquals("clob contents", ObjectConverterUtil.convertToString(((Clob)results.get(15)).getCharacterStream()));
        assertEquals("<a>foo</a>", ObjectConverterUtil.convertToString(((SQLXML)results.get(16)).getCharacterStream()));
        assertNull(exec.next());        
    }
}
