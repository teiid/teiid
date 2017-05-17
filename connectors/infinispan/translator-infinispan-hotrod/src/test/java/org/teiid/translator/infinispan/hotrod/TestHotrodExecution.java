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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

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
    }
}
