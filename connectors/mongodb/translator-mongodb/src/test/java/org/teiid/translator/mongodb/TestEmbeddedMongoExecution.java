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
package org.teiid.translator.mongodb;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.CommandContext;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;

@SuppressWarnings("nls")
public class TestEmbeddedMongoExecution {
    private static MongoDBExecutionFactory translator;
    private static TranslationUtility utility;
    private static EmbeddedMongoDB mongodb;
    private MongoClient client;
    private MongoDBConnection connection;

    @BeforeClass
    public static void setUp() throws Exception {
        translator = new MongoDBExecutionFactory();
        translator.start();

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("test.ddl")), "test", "dummy");
        utility = new TranslationUtility(metadata);

        mongodb = new EmbeddedMongoDB();

        MongoClient client = new MongoClient("localhost", 12345);
        MongoDBConnection conn = getConnection(client);
        translator.initCapabilities(conn);
        conn.close();
    }

    @Before
    public void beforeTest() throws Exception {
        this.client = new MongoClient("localhost", 12345);
        this.connection = getConnection(client);
    }

    @After
    public void afterTest() throws Exception {
        this.client.close();
    }

    private static MongoDBConnection getConnection(MongoClient client) {
        MongoDBConnection connection = Mockito.mock(MongoDBConnection.class);
        Mockito.stub(connection.getDatabase()).toReturn(client.getDB("test"));
        return connection;
    }

    public static void stop() {
        mongodb.stop();
    }

    private Execution executeCmd(String sql) throws Exception {
        Command cmd = utility.parseCommand(sql);
        CommandContext cc = Mockito.mock(CommandContext.class);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        Mockito.stub(ec.getCommandContext()).toReturn(cc);
        Execution exec =  translator.createExecution(cmd, ec, utility.createRuntimeMetadata(), this.connection);
        exec.execute();
        return exec;
    }


    @Test
    public void testSingleTableExecution() throws Exception {
        executeCmd("delete from G1");
        executeCmd("insert into G1 (e1, e2, e3) values (1, 1, 1)");
        executeCmd("insert into G1 (e1, e2, e3) values (2, 2, 2)");
        executeCmd("insert into G1 (e1, e2, e3) values (3, 3, 3)");

        MongoDBQueryExecution exec = (MongoDBQueryExecution)executeCmd("select * from G1");
        assertEquals(Arrays.asList(1, 1, 1), exec.next());
        assertEquals(Arrays.asList(2, 2, 2), exec.next());
        assertEquals(Arrays.asList(3, 3, 3), exec.next());
        assertNull(exec.next());

        executeCmd("update G1 set e2=4 where e3 >= 2");
        exec = (MongoDBQueryExecution)executeCmd("select * from G1");
        assertEquals(Arrays.asList(1, 1, 1), exec.next());
        assertEquals(Arrays.asList(2, 4, 2), exec.next());
        assertEquals(Arrays.asList(3, 4, 3), exec.next());
        assertNull(exec.next());

        executeCmd("delete from G1 where e2=4");
        exec = (MongoDBQueryExecution)executeCmd("select * from G1");
        assertEquals(Arrays.asList(1, 1, 1), exec.next());
        assertNull(exec.next());
    }

    @Test
    public void testOne2OneMerge() throws Exception {
        executeCmd("delete from G1");
        executeCmd("insert into G1 (e1, e2, e3) values (1, 1, 1)");
        executeCmd("insert into G2 (e1, e2, e3) values (1, 2, 3)");
        MongoDBQueryExecution exec = (MongoDBQueryExecution)executeCmd("select * from G2");
        assertEquals(Arrays.asList(1, 2, 3), exec.next());
        assertNull(exec.next());

        MongoClient client = new MongoClient("localhost", 12345);
        assertNull(client.getDB("test").getCollection("G2").findOne());

        BasicDBObject row = new BasicDBObject("_id", 1).append("e2", 1).append("e3", 1);
        row.append("G2", new BasicDBObject("e2", 2).append("e3", 3));
        assertEquals(row, client.getDB("test").getCollection("G1").findOne());

        exec = (MongoDBQueryExecution)executeCmd("select g1.e1, g1.e2, g2.e3 from G1 JOIN G2 ON G1.e1=G2.e1");
        assertEquals(Arrays.asList(1, 1, 3), exec.next());
        assertNull(exec.next());

        client.close();
    }

    @Test
    public void testOne2ManyMerge() throws Exception {
        executeCmd("delete from G1");
        executeCmd("insert into G1 (e1, e2, e3) values (1, 1, 1)");
        executeCmd("insert into G3 (e1, e2, e3) values (2, 1, 3)");
        MongoDBQueryExecution exec = (MongoDBQueryExecution)executeCmd("select * from G3");
        assertEquals(Arrays.asList(2, 1, 3), exec.next());
        assertNull(exec.next());

        MongoClient client = new MongoClient("localhost", 12345);
        assertNull(client.getDB("test").getCollection("G3").findOne());

        BasicDBObject row = new BasicDBObject("_id", 1).append("e2", 1).append("e3", 1);
        BasicDBList list = new BasicDBList();
        list.add(new BasicDBObject("_id", 2).append("e3", 3));
        row.append("G3", list);
        assertEquals(row, client.getDB("test").getCollection("G1").findOne());

        exec = (MongoDBQueryExecution)executeCmd("select G1.e1, G1.e2, G3.e2, G3.e3 from G1 JOIN G3 ON G1.e1=G3.e2");
        assertEquals(Arrays.asList(1, 1, 1, 3), exec.next());
        assertNull(exec.next());

        executeCmd("update G3 set e3=4 where e2=1");
        exec = (MongoDBQueryExecution)executeCmd("select G1.e1, G1.e2, G3.e2, G3.e3 from G1 JOIN G3 ON G1.e1=G3.e2");
        assertEquals(Arrays.asList(1, 1, 1, 4), exec.next());
        assertNull(exec.next());

        executeCmd("delete from G3 where G3.e2=1");

        exec = (MongoDBQueryExecution)executeCmd("select G1.e1, G1.e2, G3.e2, G3.e3 from G1 JOIN G3 ON G1.e1=G3.e2");
        assertNull(exec.next());

        client.close();
    }

    @Test
    public void testEmbedded() throws Exception {
        executeCmd("delete from G1");
        executeCmd("insert into G4 (e1, e2, e3) values (2, 2, 3)");
        executeCmd("insert into G1E (e1, e2, e3, e4) values (2, 2, 2, 2)");
        MongoDBQueryExecution exec = (MongoDBQueryExecution)executeCmd("select * from G4");
        assertEquals(Arrays.asList(2, 2, 3), exec.next());
        assertNull(exec.next());

        BasicDBObject g4_row = new BasicDBObject("_id", 2).append("e2", 2).append("e3", 3);
        MongoClient client = new MongoClient("localhost", 12345);
        assertEquals(g4_row, client.getDB("test").getCollection("G4").findOne());

        BasicDBObject row = new BasicDBObject("_id", 2).append("e2", 2).append("e3", 2).append("e4", 2);
        row.append("G4",new BasicDBObject("e2", 2).append("e3", 3));
        assertEquals(row, client.getDB("test").getCollection("G1E").findOne());

        exec = (MongoDBQueryExecution)executeCmd("select G1E.e1, G1E.e2, G4.e3 from G1E JOIN G4 ON G1E.e1=G4.e1");
        assertEquals(Arrays.asList(2, 2, 3), exec.next());
        assertNull(exec.next());

        client.close();
    }

    @Test
    public void testTimeFunction() throws Exception {
        executeCmd("delete from TIME_TEST");
        executeCmd("insert into TIME_TEST (e1) values (0)"); //missing
        executeCmd("insert into TIME_TEST (e1, e2) values (1, null)"); //null
        MongoDBQueryExecution exec = (MongoDBQueryExecution)executeCmd("select * from TIME_TEST where YEAR(e2) = 1");
        assertNull(exec.next());

        executeCmd("insert into TIME_TEST (e1, e2) values (2, '2001-01-01 01:02:03')");

        exec = (MongoDBQueryExecution)executeCmd("SELECT e2, second(e2) as sec FROM TIME_TEST WHERE second(e2) >= 0");
        assertEquals(Arrays.asList(TimestampUtil.createTimestamp(101, 0, 1, 1, 2, 3, 0), 3), exec.next());

        exec = (MongoDBQueryExecution)executeCmd("SELECT e1, e2, second(e2) as sec FROM TIME_TEST");
        assertEquals(Arrays.asList(0, null, null), exec.next());
        assertEquals(Arrays.asList(1, null, null), exec.next());
        assertEquals(Arrays.asList(2, TimestampUtil.createTimestamp(101, 0, 1, 1, 2, 3, 0), 3), exec.next());

        client.close();
    }

    @Test
    public void testGroupByHaving() throws Exception {
        executeCmd("delete from TIME_TEST");
        executeCmd("insert into TIME_TEST (e1, e2) values (1, null)");
        executeCmd("insert into TIME_TEST (e1, e2) values (2, null)");
        String sql = "SELECT max(e1), e2, count(*) FROM TIME_TEST GROUP BY e2 HAVING count(*) = 2";

        MongoDBQueryExecution exec = (MongoDBQueryExecution)executeCmd(sql);
        assertEquals(Arrays.asList(2, null, 2), exec.next());
        assertNull(exec.next());

        sql = "SELECT min(e1), e2 FROM TIME_TEST GROUP BY e2 HAVING count(*) = 2";

        exec = (MongoDBQueryExecution)executeCmd(sql);
        assertEquals(Arrays.asList(1, null), exec.next());
        assertNull(exec.next());

        //won't work as it requires a secondary projection
        sql = "SELECT min(e1)+1, e2 FROM TIME_TEST GROUP BY e2";

        /*exec = (MongoDBQueryExecution)executeCmd(sql);
        assertEquals(Arrays.asList(1), exec.next());
        assertEquals(Arrays.asList(1, null), exec.next());
        assertNull(exec.next());*/

        client.close();
    }
}
