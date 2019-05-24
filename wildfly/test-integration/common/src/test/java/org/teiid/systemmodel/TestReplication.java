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

package org.teiid.systemmodel;

import static org.junit.Assert.*;

import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.jdbc.FakeServer.DeployVDBParameter;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.TableStats;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.ReplicatedObject;
import org.teiid.runtime.ReplicatedServer;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestReplication {

    private static final String MATVIEWS = "matviews";
    private static final boolean DEBUG = false;
    private ReplicatedServer server1;
    private ReplicatedServer server2;

    @BeforeClass public static void oneTimeSetup() {
        if (DEBUG) {
            UnitTestUtil.enableTraceLogging("org.teiid");
        }
    }

    @After public void tearDown() {
        if (server1 != null) {
            server1.stop();
        }
        if (server2 != null) {
            server2.stop();
        }
    }

    @Test public void testReplication() throws Exception {
        server1 = createServer("infinispan-replicated-config.xml", "tcp-shared.xml");
        deployMatViewVDB(server1);

        Connection c1 = server1.createConnection("jdbc:teiid:matviews");
        Statement stmt = c1.createStatement();
        stmt.execute("select * from TEST.RANDOMVIEW");
        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());
        double d1 = rs.getDouble(1);
        double d2 = rs.getDouble(2);

        server2 = createServer("infinispan-replicated-config-1.xml", "tcp-shared.xml");
        deployMatViewVDB(server2);

        Connection c2 = server2.createConnection("jdbc:teiid:matviews");
        Statement stmt2 = c2.createStatement();
        ResultSet rs2 = stmt2.executeQuery("select * from matviews where name = 'RandomView'");
        assertTrue(rs2.next());
        assertEquals("LOADED", rs2.getString("loadstate"));
        assertEquals(true, rs2.getBoolean("valid"));
        stmt2.execute("select * from TEST.RANDOMVIEW");
        rs2 = stmt2.getResultSet();
        assertTrue(rs2.next());
        assertEquals(d1, rs2.getDouble(1), 0);
        assertEquals(d2, rs2.getDouble(2), 0);

        rs2 = stmt2.executeQuery("select * from (call refreshMatView('TEST.RANDOMVIEW', false)) p");

        Thread.sleep(1000);

        //make sure we're still valid and the same
        stmt.execute("select * from TEST.RANDOMVIEW");
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        d1 = rs.getDouble(1);
        d2 = rs.getDouble(2);
        stmt2.execute("select * from TEST.RANDOMVIEW");
        rs2 = stmt2.getResultSet();
        assertTrue(rs2.next());
        assertEquals(d1, rs2.getDouble(1), 0);
        assertEquals(d2, rs2.getDouble(2), 0);

        //ensure a lookup is usable on each side
        rs2 = stmt2.executeQuery("select lookup('sys.schemas', 'VDBName', 'name', 'SYS')");
        Thread.sleep(1000);

        rs = stmt.executeQuery("select lookup('sys.schemas', 'VDBName', 'name', 'SYS')");
        rs.next();
        assertEquals("matviews", rs.getString(1));

        //result set cache replication

        rs = stmt.executeQuery("/*+ cache(scope:vdb) */ select rand()"); //$NON-NLS-1$
        assertTrue(rs.next());
        d1 = rs.getDouble(1);

        //no wait is needed as we perform a synch pull
        rs2 = stmt2.executeQuery("/*+ cache(scope:vdb) */ select rand()"); //$NON-NLS-1$
        assertTrue(rs2.next());
        d2 = rs2.getDouble(1);

        assertEquals(d1, d2, 0);

        TableStats stats = new TableStats();
        stats.setCardinality(1f);
        server1.getEventDistributor().setTableStats("matviews", "1", "TEST", "RANDOMVIEW", stats);
        stmt.execute("select Cardinality from sys.tables where UPPER(name) = 'RANDOMVIEW'");
        stmt.getResultSet().next();
        long val = stmt.getResultSet().getLong(1);
        assertEquals(1, val);

        Thread.sleep(1000);

        stmt2.execute("select Cardinality from sys.tables where UPPER(name) = 'RANDOMVIEW'");
        stmt2.getResultSet().next();
        long val2 = stmt2.getResultSet().getLong(1);
        assertEquals(1, val2);
    }

    @Test(timeout=180000) public void testReplicationStartTimeout() throws Exception {
        server1 = createServer("infinispan-replicated-config.xml", "tcp-shared.xml");
        server2 = createServer("infinispan-replicated-config-1.xml", "tcp-shared.xml");

        deployMatViewVDB(server1);

        Connection c1 = server1.createConnection("jdbc:teiid:matviews");
        Statement stmt = c1.createStatement();
        stmt.execute("select * from TEST.RANDOMVIEW");
        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());

        deployMatViewVDB(server2);
    }

    @Test public void testLargeReplicationFailedTransfer() throws Exception {
        server1 = createServer("infinispan-replicated-config.xml", "tcp-shared.xml");
        deployLargeVDB(server1);

        Connection c1 = server1.createConnection("jdbc:teiid:large");
        Statement stmt = c1.createStatement();
        stmt.execute("select * from c");
        ResultSet rs = stmt.getResultSet();
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
        }

        Thread.sleep(1000);

        server2 = createServer("infinispan-replicated-config-1.xml", "tcp-shared.xml");

        //add a replicator to kill transfers
        final ObjectReplicator or = server2.getObjectReplicator();
        server2.setObjectReplicator(new ObjectReplicator() {

            @Override
            public void stop(Object o) {

            }

            @Override
            public <T, S> T replicate(String id, Class<T> iface, final S object,
                    long startTimeout) throws Exception {
                Object o = Proxy.newProxyInstance(TestReplication.class.getClassLoader(), new Class<?>[] {iface, ReplicatedObject.class},  new InvocationHandler() {

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args)
                            throws Throwable {
                        if (method.getName().equals("setState")) {
                            ((InputStream)args[args.length - 1]).close();
                        }
                        try {
                            return method.invoke(object, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    }
                });
                return or.replicate(id, iface, o, startTimeout);
            }
        });
        deployLargeVDB(server2);

        Connection c2 = server2.createConnection("jdbc:teiid:large");

        Statement stmt2 = c2.createStatement();
        ResultSet rs2 = stmt2.executeQuery("select * from matviews where name = 'c'");
        assertTrue(rs2.next());
        assertEquals("NEEDS_LOADING", rs2.getString("loadstate"));

        stmt2 = c2.createStatement();
        rs2 = stmt2.executeQuery("select * from c");

        int rowCount2 = 0;
        while (rs2.next()) {
            rowCount2++;
        }

        assertEquals(rowCount, rowCount2);
    }

    @Test public void testLargeReplication() throws Exception {
        server1 = createServer("infinispan-replicated-config.xml", "tcp-shared.xml");
        deployLargeVDB(server1);

        Connection c1 = server1.createConnection("jdbc:teiid:large");
        Statement stmt = c1.createStatement();
        stmt.execute("select * from c");
        ResultSet rs = stmt.getResultSet();
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
        }

        Thread.sleep(1000);

        server2 = createServer("infinispan-replicated-config-1.xml", "tcp-shared.xml");
        deployLargeVDB(server2);

        Connection c2 = server2.createConnection("jdbc:teiid:large");

        Statement stmt2 = c2.createStatement();
        ResultSet rs2 = stmt2.executeQuery("select * from matviews where name = 'c'");
        assertTrue(rs2.next());
        assertEquals("LOADED", rs2.getString("loadstate"));

        stmt2 = c2.createStatement();
        rs2 = stmt2.executeQuery("select * from c");

        int rowCount2 = 0;
        while (rs2.next()) {
            rowCount2++;
        }

        assertEquals(rowCount, rowCount2);
    }

    @Test public void testLazyTtl() throws Exception {
        server1 = createServer("infinispan-replicated-config.xml", "tcp-shared.xml");
        deployTtlVDB(server1);

        server2 = createServer("infinispan-replicated-config-1.xml", "tcp-shared.xml");
        deployTtlVDB(server2);

        Thread.sleep(1000);

        Connection c1 = server1.createConnection("jdbc:teiid:ttl");

        Statement stmt = c1.createStatement();
        //force the load
        stmt.execute("select * from c");
        ResultSet rs2 = stmt.executeQuery("select * from matviews where name = 'c'");
        assertTrue(rs2.next());
        assertEquals("LOADED", rs2.getString("loadstate"));

        //expire the ttl
        Thread.sleep(2000);

        //trigger the refresh
        stmt.execute("select * from c");

        //expire the ttl
        Thread.sleep(2000);

        //make sure that it's still accessible
        stmt.execute("select * from c");
    }

    private ReplicatedServer createServer(String ispn, String jgroups) throws Exception {
        return ReplicatedServer.createServer(null, ispn, jgroups);
    }

    private void deployTtlVDB(ReplicatedServer server)
            throws ConnectorManagerException, VirtualDatabaseException,
            TranslatorException {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("mv");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create view c options (materialized true) as /*+ cache(ttl:1000) */ select 'hello world'");
        VDBMetaData vdb = new VDBMetaData();
        vdb.setXmlDeployment(true);
        vdb.setName("ttl");
        vdb.setModels(Arrays.asList(mmd));
        vdb.addProperty("lazy-invalidate", "true");
        server.deployVDB(vdb);
    }

    private void deployLargeVDB(ReplicatedServer server)
            throws ConnectorManagerException, VirtualDatabaseException,
            TranslatorException {
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("mv");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create view c options (materialized true) as WITH t(n) AS ( VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 10000 ) SELECT n, n || 'a', n + n FROM t");

        server.deployVDB("large", mmd);
    }

    private void deployMatViewVDB(ReplicatedServer server) throws Exception {
        HashMap<String, Collection<FunctionMethod>> udfs = new HashMap<String, Collection<FunctionMethod>>();
        udfs.put("funcs", Arrays.asList(new FunctionMethod("pause", null, null, PushDown.CANNOT_PUSHDOWN, TestMatViews.class.getName(), "pause", null, new FunctionParameter("return", DataTypeManager.DefaultDataTypes.INTEGER), true, Determinism.NONDETERMINISTIC)));
        server.deployVDB(MATVIEWS, UnitTestUtil.getTestDataPath() + "/matviews.vdb", new DeployVDBParameter(udfs, null));
    }

}
