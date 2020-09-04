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

package org.teiid.runtime;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.transaction.*;
import javax.transaction.xa.XAResource;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.postgresql.Driver;
import org.teiid.CommandContext;
import org.teiid.GeneratedKeys;
import org.teiid.PreParser;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.impl.BufferFrontedFileStoreCache;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.common.buffer.impl.SplittableStorageManager;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.SimpleMock;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.jdbc.SQLStates;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.jdbc.tracing.GlobalTracerInjector;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.QueryExpression;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.Logger;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.resource.api.XAImporter;
import org.teiid.runtime.EmbeddedServer.ConnectionFactoryProvider;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorBatchException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.UpdateExecution;
import org.teiid.transport.SSLConfiguration;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.WireProtocol;

import io.opentracing.Scope;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.GlobalTracer;

@SuppressWarnings("nls")
public class TestEmbeddedServer {
    @Translator (name="y")
    public static class FakeTranslator extends
            ExecutionFactory<AtomicInteger, Object> {

        private boolean batch;

        public FakeTranslator() {
            this.batch = false;
        }

        public FakeTranslator(boolean batch) {
            this.batch = batch;
        }

        @Override
        public Object getConnection(AtomicInteger factory)
                throws TranslatorException {
            return factory.incrementAndGet();
        }

        @Override
        public void closeConnection(Object connection, AtomicInteger factory) {

        }

        @Override
        public boolean supportsBulkUpdate() {
            return true;
        }

        @Override
        public void getMetadata(MetadataFactory metadataFactory, Object conn)
                throws TranslatorException {
            assertEquals(conn, Integer.valueOf(1));
            Table t = metadataFactory.addTable("my-table");
            t.setSupportsUpdate(true);
            Column c = metadataFactory.addColumn("my-column", TypeFacility.RUNTIME_NAMES.STRING, t);
            c.setUpdatable(true);
        }

        @Override
        public ResultSetExecution createResultSetExecution(
                QueryExpression command, ExecutionContext executionContext,
                RuntimeMetadata metadata, Object connection)
                throws TranslatorException {
            ResultSetExecution rse = new ResultSetExecution() {

                @Override
                public void execute() throws TranslatorException {

                }

                @Override
                public void close() {

                }

                @Override
                public void cancel() throws TranslatorException {

                }

                @Override
                public List<?> next() throws TranslatorException, DataNotAvailableException {
                    return null;
                }
            };
            return rse;
        }

        @Override
        public UpdateExecution createUpdateExecution(Command command,
                ExecutionContext executionContext,
                RuntimeMetadata metadata, Object connection)
                throws TranslatorException {
            UpdateExecution ue = new UpdateExecution() {

                @Override
                public void execute() throws TranslatorException {

                }

                @Override
                public void close() {

                }

                @Override
                public void cancel() throws TranslatorException {

                }

                @Override
                public int[] getUpdateCounts() throws DataNotAvailableException,
                        TranslatorException {
                    if (!batch) {
                        return new int[] {2};
                    }
                    return new int[] {1, 1, -1, 1, 1, 1, -1, 1, 1, 1, -1, 1, 1, 1, -1, 1};
                }
            };
            return ue;
        }

        @Override
        public boolean isSourceRequiredForMetadata() {
            return false;
        }
    }

    public static final class MockTransactionManager implements TransactionManager {
        private class MockTransaction implements Transaction {
            int status = Status.STATUS_ACTIVE;
            private List<Synchronization> synchronizations = new ArrayList<>();
            boolean addedSynchronization;

            @Override
            public void setRollbackOnly()
                    throws IllegalStateException, SystemException {
                if (status == Status.STATUS_ACTIVE
                        || status == Status.STATUS_MARKED_ROLLBACK
                        || status == Status.STATUS_ROLLEDBACK) {
                    status = Status.STATUS_MARKED_ROLLBACK;
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public void rollback() throws IllegalStateException, SystemException {
                if (status == Status.STATUS_COMMITTED) {
                    throw new IllegalStateException();
                }
                for (Synchronization synchronization : synchronizations) {
                    synchronization.beforeCompletion();
                }
                status = Status.STATUS_ROLLEDBACK;
                for (Synchronization synchronization : synchronizations) {
                    synchronization.afterCompletion(status);
                }
                synchronizations.clear();
            }

            @Override
            public void registerSynchronization(Synchronization sync)
                    throws RollbackException, IllegalStateException, SystemException {
                this.synchronizations.add(sync);
                addedSynchronization = true;
            }

            @Override
            public int getStatus() throws SystemException {
                return status;
            }

            @Override
            public boolean enlistResource(XAResource xaRes)
                    throws RollbackException, IllegalStateException, SystemException {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean delistResource(XAResource xaRes, int flag)
                    throws IllegalStateException, SystemException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void commit() throws RollbackException, HeuristicMixedException,
                    HeuristicRollbackException, SecurityException,
                    IllegalStateException, SystemException {
                if (status == Status.STATUS_ACTIVE) {
                    for (Synchronization synchronization : synchronizations) {
                        synchronization.beforeCompletion();
                    }
                    status = Status.STATUS_COMMITTED;
                    for (Synchronization synchronization : synchronizations) {
                        synchronization.afterCompletion(status);
                    }
                    synchronizations.clear();
                } else if (status == Status.STATUS_MARKED_ROLLBACK || status == Status.STATUS_ROLLEDBACK) {
                    throw new RollbackException();
                } else {
                    throw new IllegalStateException();
                }
            }
        }

        ThreadLocal<Transaction> txns = new ThreadLocal<Transaction>();
        List<MockTransaction> txnHistory = new ArrayList<>();

        @Override
        public Transaction suspend() throws SystemException {
            Transaction result = txns.get();
            txns.remove();
            return result;
        }

        @Override
        public void setTransactionTimeout(int seconds) throws SystemException {
        }

        @Override
        public void setRollbackOnly() throws IllegalStateException, SystemException {
            Transaction result = txns.get();
            if (result == null) {
                throw new IllegalStateException();
            }
            result.setRollbackOnly();
        }

        @Override
        public void rollback() throws IllegalStateException, SecurityException,
                SystemException {
            Transaction t = checkNull(false);
            txns.remove();
            t.rollback();
        }

        @Override
        public void resume(Transaction tobj) throws InvalidTransactionException,
                IllegalStateException, SystemException {
            checkNull(true);
            txns.set(tobj);
        }

        private Transaction checkNull(boolean isNull) {
            Transaction t = txns.get();
            if ((!isNull && t == null) || (isNull && t != null)) {
                throw new IllegalStateException();
            }
            return t;
        }

        @Override
        public Transaction getTransaction() throws SystemException {
            return txns.get();
        }

        @Override
        public int getStatus() throws SystemException {
            Transaction t = txns.get();
            if (t == null) {
                return javax.transaction.Status.STATUS_NO_TRANSACTION;
            }
            return t.getStatus();
        }

        @Override
        public void commit() throws RollbackException, HeuristicMixedException,
                HeuristicRollbackException, SecurityException,
                IllegalStateException, SystemException {
            Transaction t = checkNull(false);
            txns.remove();
            t.commit();
        }

        @Override
        public void begin() throws NotSupportedException, SystemException {
            checkNull(true);
            MockTransaction t = new MockTransaction();
            txnHistory.add(t);
            txns.set(t);
        }

        public void reset() {
            txnHistory.clear();
            txns = new ThreadLocal<Transaction>();
        }
    }

    EmbeddedServer es;

    @Before public void setup() {
        es = new EmbeddedServer();
    }

    @After public void teardown() {
        if (es != null) {
            es.stop();
        }
    }

    @Test(expected=VirtualDatabaseException.class) public void testDeployInformationSchema() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.start(ec);

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("information_schema");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.addSourceMetadata("DDL", "create view v as select 1;");

        es.deployVDB("test", mmd1);
    }

    @Test public void testDeploy() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.start(ec);

        es.addTranslator("y", new FakeTranslator(false));
        final AtomicInteger counter = new AtomicInteger();
        ConnectionFactoryProvider<AtomicInteger> cfp = new EmbeddedServer.SimpleConnectionFactoryProvider<AtomicInteger>(counter);

        es.addConnectionFactoryProvider("z", cfp);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("my-schema");
        mmd.addSourceMapping("x", "y", "z");

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("virt");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.setSchemaSourceType("ddl");
        mmd1.setSchemaText("create view \"my-view\" OPTIONS (UPDATABLE 'true') as select * from \"my-table\"");

        es.deployVDB("test", mmd, mmd1);

        TeiidDriver td = es.getDriver();
        Connection c = td.connect("jdbc:teiid:test", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from \"my-view\"");
        assertFalse(rs.next());
        assertEquals("my-column", rs.getMetaData().getColumnLabel(1));

        s.execute("update \"my-view\" set \"my-column\" = 'a'");
        assertEquals(2, s.getUpdateCount());

        es.deployVDB("empty");
        c = es.getDriver().connect("jdbc:teiid:empty", null);
        s = c.createStatement();
        s.execute("select * from sys.tables");

        assertNotNull(es.getSchemaDdl("empty", "SYS"));
        assertNull(es.getSchemaDdl("empty", "xxx"));
    }

    @Test public void testBatchedUpdate() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();

        ec.setUseDisk(false);
        es.bufferService.setProcessorBatchSize(1);
        es.start(ec);

        es.addTranslator("y", new FakeTranslator(true));
        final AtomicInteger counter = new AtomicInteger();
        ConnectionFactoryProvider<AtomicInteger> cfp = new EmbeddedServer.SimpleConnectionFactoryProvider<AtomicInteger>(counter);

        es.addConnectionFactoryProvider("z", cfp);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("my-schema");
        mmd.addSourceMapping("x", "y", "z");
        es.deployVDB("test", mmd);

        Connection c = es.getDriver().connect("jdbc:teiid:test", null);
        PreparedStatement ps = c.prepareStatement("insert into \"my-table\" values (?)");
        for (int i = 0; i < 16; i++) {
        ps.setString(1, "a");
        ps.addBatch();
        }
        int[] result = ps.executeBatch();
        assertArrayEquals(new int[] {1, 1, -1, 1, 1, 1, -1, 1, 1, 1, -1, 1, 1, 1, -1, 1}, result);
    }

    @Test(expected=VirtualDatabaseException.class)
    public void testInvalidName() throws Exception {
        es.start(new EmbeddedConfiguration());
        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("virt.1");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.setSchemaSourceType("ddl");
        mmd1.setSchemaText("create view \"my-view\" as select 1");
        es.deployVDB("x", mmd1);
    }

    @Test public void testDeployZip() throws Exception {
        es.start(new EmbeddedConfiguration());

        File f = UnitTestUtil.getTestScratchFile("some.vdb");
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
        out.putNextEntry(new ZipEntry("v1.ddl"));
        out.write("CREATE VIEW helloworld as SELECT 'HELLO WORLD';".getBytes("UTF-8"));
        out.putNextEntry(new ZipEntry("META-INF/vdb.xml"));
        out.write("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL-FILE\">/v1.ddl</metadata></model></vdb>".getBytes("UTF-8"));
        out.close();

        es.getAdmin().deployVDBZip(f.toURI().toURL());
        ResultSet rs = es.getDriver().connect("jdbc:teiid:test", null).createStatement().executeQuery("select * from helloworld");
        rs.next();
        assertEquals("HELLO WORLD", rs.getString(1));
    }

    @Test public void testDeployZipDDL() throws Exception {
        es.start(new EmbeddedConfiguration());

        File f = UnitTestUtil.getTestScratchFile("some.vdb");
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
        out.putNextEntry(new ZipEntry("v1.ddl"));
        out.write("CREATE VIEW helloworld as SELECT 'HELLO WORLD';".getBytes("UTF-8"));
        out.putNextEntry(new ZipEntry("META-INF/vdb.ddl"));
        String externalDDL = "CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "IMPORT FROM REPOSITORY \"DDL-FILE\" INTO test2 OPTIONS(\"ddl-file\" '/v1.ddl');";
        out.write(externalDDL.getBytes("UTF-8"));
        out.close();

        es.deployVDBZip(f.toURI().toURL());
        ResultSet rs = es.getDriver().connect("jdbc:teiid:test", null).createStatement().executeQuery("select * from helloworld");
        rs.next();
        assertEquals("HELLO WORLD", rs.getString(1));
    }

    @Test public void testDDLVDBRenameTable() throws Exception {
        es.start(new EmbeddedConfiguration());

        String ddl1 = "CREATE DATABASE x VERSION '1';"
                + "USE DATABASE x VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "SET SCHEMA test2;"
                + "CREATE VIEW x as select 1;"
                + "ALTER VIEW x RENAME TO y;";

        es.getAdmin().deploy("x-vdb.ddl", new ByteArrayInputStream(ddl1.getBytes("UTF-8")));

        ResultSet rs = es.getDriver().connect("jdbc:teiid:x", null).createStatement().executeQuery("select * from y");
        rs.next();
        assertEquals("1", rs.getString(1));
    }

    @Test public void testDDLVDBAddColumn() throws Exception {
        es.start(new EmbeddedConfiguration());

        String ddl1 = "CREATE DATABASE x VERSION '1';"
                + "USE DATABASE x VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "SET SCHEMA test2;"
                + "CREATE VIRTUAL VIEW x (col string) as select 'a';"
                + "ALTER VIEW x ADD COLUMN y decimal;"
                + "ALTER VIEW x AS select 'a', 1.1;";

        es.deployVDB(new ByteArrayInputStream(ddl1.getBytes("UTF-8")), true);

        ResultSet rs = es.getDriver().connect("jdbc:teiid:x", null).createStatement().executeQuery("select * from x");
        rs.next();
        assertEquals("a", rs.getString(1));
        assertEquals(1.1, rs.getDouble(2), 0);
    }

    @Test public void testDDLVDBImport() throws Exception {
        es.start(new EmbeddedConfiguration());

        String ddl1 = "CREATE DATABASE x VERSION '1';"
                + "USE DATABASE x VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "SET SCHEMA test2;"
                + "CREATE VIEW x as select 1;";

        String ddl2 = "CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "IMPORT DATABASE x VERSION '1';";

        es.deployVDB(new ByteArrayInputStream(ddl1.getBytes("UTF-8")), true);
        es.deployVDB(new ByteArrayInputStream(ddl2.getBytes("UTF-8")), true);
    }

    @Test public void testDDLVDBImportTransitive() throws Exception {
        es.start(new EmbeddedConfiguration());

        String ddl1 = "CREATE DATABASE x VERSION '1';"
                + "USE DATABASE x VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "SET SCHEMA test2;"
                + "CREATE VIEW x as select 1;";

        String ddl2 = "CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "IMPORT DATABASE x VERSION '1';";

        String ddl3 = "CREATE DATABASE test2 VERSION '1';"
                + "USE DATABASE test2 VERSION '1';"
                + "IMPORT DATABASE x VERSION '1';";

        String ddl4 = "CREATE DATABASE test3 VERSION '1';"
                + "USE DATABASE test3 VERSION '1';"
                + "IMPORT DATABASE test VERSION '1';"
                + "IMPORT DATABASE test2 VERSION '1';";

        es.deployVDB(new ByteArrayInputStream(ddl1.getBytes("UTF-8")), true);
        es.deployVDB(new ByteArrayInputStream(ddl2.getBytes("UTF-8")), true);
        es.deployVDB(new ByteArrayInputStream(ddl3.getBytes("UTF-8")), true);
        es.deployVDB(new ByteArrayInputStream(ddl4.getBytes("UTF-8")), true);

        ResultSet rs = es.getDriver().connect("jdbc:teiid:test3", null).createStatement().executeQuery("select * from x");
        rs.next();
        assertEquals("1", rs.getString(1));
    }

    @Test public void testDDLVDBImportTransitiveFanout() throws Exception {
        es.start(new EmbeddedConfiguration());

        String base1 = "CREATE DATABASE x VERSION '1';"
                + "USE DATABASE x VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test1;"
                + "SET SCHEMA test1;"
                + "CREATE VIEW x as select 1;";

        String base2 = "CREATE DATABASE y VERSION '1';"
                + "USE DATABASE y VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "SET SCHEMA test2;"
                + "CREATE VIEW y as select 1;";

        String intermediate = "CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "IMPORT DATABASE x VERSION '1';"
                + "IMPORT DATABASE y VERSION '1';";

        String top = "CREATE DATABASE test2 VERSION '1';"
                + "USE DATABASE test2 VERSION '1';"
                + "IMPORT DATABASE test VERSION '1';";

        es.deployVDB(new ByteArrayInputStream(base1.getBytes("UTF-8")), true);
        es.deployVDB(new ByteArrayInputStream(base2.getBytes("UTF-8")), true);
        es.deployVDB(new ByteArrayInputStream(intermediate.getBytes("UTF-8")), true);
        es.deployVDB(new ByteArrayInputStream(top.getBytes("UTF-8")), true);

        ResultSet rs = es.getDriver().connect("jdbc:teiid:test2", null).createStatement().executeQuery("select * from x");
        rs.next();
        assertEquals("1", rs.getString(1));
    }

    @Test(expected=MetadataException.class) public void testAlterImported() throws Exception {
        es.start(new EmbeddedConfiguration());

        String ddl1 = "CREATE DATABASE x VERSION '1';"
                + "USE DATABASE x VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "SET SCHEMA test2;"
                + "CREATE VIEW x as select 1;";

        String ddl2 = "CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "IMPORT DATABASE x VERSION '1';"
                + "set schema test2;"
                + "DROP VIEW x;";

        es.deployVDB(new ByteArrayInputStream(ddl1.getBytes("UTF-8")), true);
        es.deployVDB(new ByteArrayInputStream(ddl2.getBytes("UTF-8")), true);
    }

    @Test public void testDeployDesignerZip() throws Exception {
        es.start(new EmbeddedConfiguration());
        es.deployVDBZip(UnitTestUtil.getTestDataFile("matviews.vdb").toURI().toURL());
        ResultSet rs = es.getDriver().connect("jdbc:teiid:matviews", null).createStatement().executeQuery("select count(*) from sys.tables where schemaname='test'");
        rs.next();
        assertEquals(4, rs.getInt(1));
    }

    @Test public void testXMLDeploy() throws Exception {
        es.start(new EmbeddedConfiguration());
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE VIEW helloworld as SELECT 'HELLO WORLD';]]> </metadata></model></vdb>".getBytes()));
        ResultSet rs =es.getDriver().connect("jdbc:teiid:test", null).createStatement().executeQuery("select * from helloworld");
        rs.next();
        assertEquals("HELLO WORLD", rs.getString(1));
    }

    @Test public void testXMLDeployWithVDBImport() throws Exception {
        es.start(new EmbeddedConfiguration());
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE VIEW helloworld as SELECT 'HELLO WORLD';]]> </metadata></model></vdb>".getBytes()));
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"importer\" version=\"1\"><import-vdb name=\"test\" version=\"1\"/></vdb>".getBytes()));
        ResultSet rs =es.getDriver().connect("jdbc:teiid:importer", null).createStatement().executeQuery("select * from helloworld");
        rs.next();
        assertEquals("HELLO WORLD", rs.getString(1));
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"importer1\" version=\"1\"><import-vdb name=\"importer\" version=\"1\"/></vdb>".getBytes()));
        rs =es.getDriver().connect("jdbc:teiid:importer1", null).createStatement().executeQuery("select * from helloworld");
        rs.next();
        assertEquals("HELLO WORLD", rs.getString(1));
    }

    @Test
    public void testRemoteJDBCTrasport() throws Exception {
        SocketConfiguration s = new SocketConfiguration();
        InetSocketAddress addr = new InetSocketAddress(0);
        s.setBindAddress(addr.getHostName());
        s.setPortNumber(addr.getPort());
        s.setProtocol(WireProtocol.teiid);
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.addTransport(s);
        es.start(config);
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE VIEW helloworld as SELECT 'HELLO WORLD';]]> </metadata></model></vdb>".getBytes()));
        Connection conn = null;
        try {
            TeiidDriver driver = new TeiidDriver();
            conn = driver.connect("jdbc:teiid:test@mm://"+addr.getHostName()+":"+es.transports.get(0).getPort(), null);
            conn.createStatement().execute("set showplan on"); //trigger alternative serialization for byte count
            ResultSet rs = conn.createStatement().executeQuery("select * from helloworld");
            rs.next();
            assertEquals("HELLO WORLD", rs.getString(1));
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test(expected=TeiidRuntimeException.class)
    public void testRemoteTrasportSSLFail() throws Exception {
        SocketConfiguration s = new SocketConfiguration();
        InetSocketAddress addr = new InetSocketAddress(0);
        s.setBindAddress(addr.getHostName());
        s.setPortNumber(addr.getPort());
        s.setProtocol(WireProtocol.teiid);
        SSLConfiguration sslConfiguration = new SSLConfiguration();
        sslConfiguration.setSslProtocol("x");
        sslConfiguration.setMode("enabled");
        s.setSSLConfiguration(sslConfiguration);
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.addTransport(s);
        es.start(config);
    }

    @Test public void testRemoteODBCTrasport() throws Exception {
        SocketConfiguration s = new SocketConfiguration();
        InetSocketAddress addr = new InetSocketAddress(0);
        s.setBindAddress(addr.getHostName());
        s.setPortNumber(addr.getPort());
        s.setProtocol(WireProtocol.pg);
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.addTransport(s);
        es.start(config);
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE VIEW helloworld as SELECT 'HELLO WORLD';]]> </metadata></model></vdb>".getBytes()));
        Connection conn = null;
        try {
            Driver d = new Driver();
            Properties p = new Properties();
            p.setProperty("user", "testuser");
            p.setProperty("password", "testpassword");

            conn = d.connect("jdbc:postgresql://"+addr.getHostName()+":"+es.transports.get(0).getPort()+"/test", p);
            ResultSet rs = conn.createStatement().executeQuery("select * from helloworld");
            rs.next();
            assertEquals("HELLO WORLD", rs.getString(1));
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test(expected=VirtualDatabaseException.class) public void testXMLDeployFails() throws Exception {
        es.start(new EmbeddedConfiguration());
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE VIEW helloworld as SELECT 'HELLO WORLD';]]> </metadata></model><translator name=\"foo\" type=\"h2\"></translator></vdb>".getBytes()));
    }

    /**
     * Ensures schema validation is performed
     * @throws Exception
     */
    @Test(expected=VirtualDatabaseException.class) public void testXMLDeployFails1() throws Exception {
        es.start(new EmbeddedConfiguration());
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\"><source/></model><translator name=\"foo\" type=\"h2\"></translator></vdb>".getBytes()));
    }

    @Test(expected=VirtualDatabaseException.class) public void testDeploymentError() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.start(ec);

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("virt");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.setSchemaSourceType("ddl");
        mmd1.setSchemaText("create view \"my-view\" OPTIONS (UPDATABLE 'true') as select * from \"my-table\"");

        es.deployVDB("test", mmd1);
    }

    @Test public void testValidationOrder() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.start(ec);

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.setSchemaSourceType("ddl");
        mmd1.setSchemaText("create view v as select 1");

        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("a");
        mmd2.setModelType(Type.VIRTUAL);
        mmd2.setSchemaSourceType("ddl");
        mmd2.setSchemaText("create view v1 as select * from v");

        //We need mmd1 to validate before mmd2, reversing the order will result in an exception
        es.deployVDB("test", mmd1, mmd2);

        try {
            es.deployVDB("test2", mmd2, mmd1);
            fail();
        } catch (VirtualDatabaseException e) {

        }
    }

    @Test public void testTransactions() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);
        ec.setUseDisk(false);
        es.start(ec);

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.setSchemaSourceType("ddl");
        mmd1.setSchemaText("create view v as select 1; " +
                "create virtual procedure proc () options (updatecount 2) as begin select * from v; end; " +
                "create virtual procedure proc0 () as begin atomic select * from v; end; " +
                "create virtual procedure proc1 () as begin atomic insert into #temp values (1); insert into #temp values (1); end; " +
                "create virtual procedure proc2 (x integer) as begin atomic insert into #temp values (1); insert into #temp values (1); select 1; begin select 1/x; end exception e end; " +
                "create virtual procedure proc3 (x integer) as begin begin atomic call proc (); select 1; end create local temporary table x (y string); begin atomic call proc (); select 1; end end;");

        es.deployVDB("test", mmd1);

        TeiidDriver td = es.getDriver();
        Connection c = td.connect("jdbc:teiid:test", null);
        //local txn
        c.setAutoCommit(false);
        Statement s = c.createStatement();
        s.execute("select 1");
        c.setAutoCommit(true);
        assertEquals(1, tm.txnHistory.size());
        MockTransactionManager.MockTransaction txn = tm.txnHistory.remove(0);
        assertEquals(Status.STATUS_COMMITTED, txn.status);

        //should be an auto-commit txn (could also force with autoCommitTxn=true)
        s.execute("call proc ()");

        assertEquals(1, tm.txnHistory.size());
        txn = tm.txnHistory.remove(0);
        assertEquals(Status.STATUS_COMMITTED, txn.status);

        //no txn needed
        s.execute("call proc0()");

        assertEquals(0, tm.txnHistory.size());

        //block txn
        s.execute("call proc1()");

        assertEquals(1, tm.txnHistory.size());
        txn = tm.txnHistory.remove(0);
        assertEquals(Status.STATUS_COMMITTED, txn.status);

        s.execute("set autoCommitTxn on");
        s.execute("set noexec on");
        s.execute("select 1");
        assertFalse(s.getResultSet().next());

        s.execute("set autoCommitTxn off");
        s.execute("set noexec off");
        s.execute("call proc2(0)");
        //verify that the block txn was committed because the exception was caught
        assertEquals(1, tm.txnHistory.size());
        txn = tm.txnHistory.remove(0);
        assertEquals(Status.STATUS_ROLLEDBACK, txn.status);

        //test detection
        tm.txnHistory.clear();
        tm.begin();
        try {
            c.setAutoCommit(false);
            s.execute("select 1"); //needed since we lazily start the transaction
            fail("should fail since we aren't allowing a nested transaction");
        } catch (TeiidSQLException e) {
        }
        txn = tm.txnHistory.remove(0);
        assertEquals(Status.STATUS_ACTIVE, txn.status);

        tm.commit();
        c.setAutoCommit(true);

        tm.txnHistory.clear();
        //ensure that we properly reset the txn context
        s.execute("call proc3(0)");
        assertEquals(2, tm.txnHistory.size());
        txn = tm.txnHistory.remove(0);
        assertFalse(txn.addedSynchronization);
    }

    @Test public void testTransactionWithCatchBlocks() throws Exception {
        String ddl = "create procedure px1() returns (a string) as\n" +
                "          begin atomic\n" +
                "            begin\n" +
                "              error 'aaaa';\n" +
                "            end\n" +
                "            exception e\n" +
                "            select 'bbbbb';\n" +
                "          end;\n" +
                "          create procedure px2() returns (a string) as\n" +
                "          begin atomic\n" +
                "            select 'bbbbb';\n" +
                "          end;"
                + "    create foreign table batch_test (a varchar) options (updatable true);";

        String sql = "begin \n" +
                "  loop on (select s.a as a from (call procs.px1()) as s) as x \n" +
                "  begin \n" +
                "    insert into batch_test (a) values (x.a); \n" +
                "  end \n" +
                "end;";

        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);
        ec.setUseDisk(false);
        es.start(ec);
        HardCodedExecutionFactory ef = new HardCodedExecutionFactory();
        ef.addUpdate("INSERT INTO batch_test (a) VALUES ('bbbbb')", new int[] {1});
        es.addTranslator("t", ef);

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("procs");
        mmd1.addSourceMetadata("ddl", ddl);
        mmd1.addSourceMapping("t", "t", null);

        es.deployVDB("vdb", mmd1);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);

        Statement s = c.createStatement();
        s.execute(sql);

        s.execute("set autoCommitTxn on");
        try {
            s.execute(sql);
            fail();
        } catch (SQLException e) {

        }
        assertNotNull(s.getWarnings());
    }

    @Test public void testMultiSourcePreparedDynamicUpdate() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);
        ec.setUseDisk(false);
        es.start(ec);

        es.addTranslator("t", new ExecutionFactory<Void, Void>());

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.setSchemaSourceType("ddl");
        mmd1.setSchemaText("create view v (i integer) OPTIONS (UPDATABLE true) as select 1; " +
                "create trigger on v instead of update as for each row begin atomic " +
                "IF (CHANGING.i)\n" +
                "EXECUTE IMMEDIATE 'select \"new\".i'; " +
                "end; ");
        mmd1.setSupportsMultiSourceBindings(true);
        mmd1.addSourceMapping("x", "t", null);
        mmd1.addSourceMapping("y", "t", null);

        es.deployVDB("vdb", mmd1);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
        PreparedStatement ps = c.prepareStatement("update v set i = ? where i = ?");
        ps.setInt(1, 2);
        ps.setInt(2, 1);
        assertEquals(1, ps.executeUpdate());
        ps.setInt(1, 3);
        ps.setInt(2, 1);
        assertEquals(1, ps.executeUpdate());
    }

    @Test public void testGeneratedKeysVirtualNone() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);
        ec.setUseDisk(false);
        es.start(ec);

        es.addTranslator("t", new ExecutionFactory<Void, Void>());

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.addSourceMetadata("ddl", "create view v (i integer, k integer auto_increment primary key) OPTIONS (UPDATABLE true) as select 1, 2; " +
                "create trigger on v instead of insert as for each row begin atomic " +
                "\ncreate local temporary table x (y serial, z integer, primary key (y));"
                + "\ninsert into x (z) values (1);" +
                "end; ");

        es.deployVDB("vdb", mmd1);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
        PreparedStatement ps = c.prepareStatement("insert into v (i) values (1)", Statement.RETURN_GENERATED_KEYS);
        assertEquals(1, ps.executeUpdate());
        ResultSet rs = ps.getGeneratedKeys();
        assertFalse(rs.next());
    }

    @Test public void testGeneratedKeysVirtual() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);
        ec.setUseDisk(false);
        es.start(ec);

        HardCodedExecutionFactory hcef = new HardCodedExecutionFactory() {
            @Override
            public UpdateExecution createUpdateExecution(Command command,
                    ExecutionContext executionContext, RuntimeMetadata metadata,
                    Object connection) throws TranslatorException {
                UpdateExecution ue = super.createUpdateExecution(command, executionContext, metadata, connection);
                GeneratedKeys keys = executionContext.getCommandContext().returnGeneratedKeys(new String[] {"y"}, new Class<?>[] {Integer.class});
                keys.addKey(Arrays.asList(1));
                return ue;
            }
        };
        hcef.addUpdate("INSERT INTO tbl (x) VALUES (1)", new int[] {1});
        es.addTranslator("t", hcef);

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.addSourceMapping("b", "t", null);
        mmd1.addSourceMetadata("ddl",
                "create foreign table tbl (x integer, y integer auto_increment primary key) OPTIONS (UPDATABLE true);" +
                "create view v (i integer, k integer auto_increment primary key) OPTIONS (UPDATABLE true) as select x, y from tbl;"+
                "create view v1 (i integer, k integer not null auto_increment primary key) OPTIONS (UPDATABLE true) as select x, y from tbl;"+
                "create trigger on v1 instead of insert as for each row begin atomic "
                + "insert into tbl (x) values (new.i); key.k = cast(generated_key('y') as integer); end;" +
                "create view v2 (i integer, k integer auto_increment primary key) OPTIONS (UPDATABLE true) as select x, y from tbl;"+
                "create trigger on v1 instead of insert as for each row begin atomic "
                + "insert into tbl (x) values (new.i); key.k = cast(generated_key('y') as integer); end;");

        es.deployVDB("vdb", mmd1);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
        PreparedStatement ps = c.prepareStatement("insert into v (i) values (1)", Statement.RETURN_GENERATED_KEYS);
        assertEquals(1, ps.executeUpdate());
        ResultSet rs = ps.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals("k", rs.getMetaData().getColumnLabel(1));

        /**
         * TODO: some systems when using not null on auto increment allow a null insert to
         */
        ps = c.prepareStatement("insert into v1 (i) values (1)", Statement.RETURN_GENERATED_KEYS);
        assertEquals(1, ps.executeUpdate());
        rs = ps.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals("k", rs.getMetaData().getColumnLabel(1));

        /**
         * Not null on v2.k should not be required for this to work.
         */
        ps = c.prepareStatement("insert into v2 (i) values (1)", Statement.RETURN_GENERATED_KEYS);
        assertEquals(1, ps.executeUpdate());
        rs = ps.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals("k", rs.getMetaData().getColumnLabel(1));
    }

    @Test public void testGeneratedKeysTemp() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);
        ec.setUseDisk(false);
        es.start(ec);

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.addSourceMetadata("ddl", "create view v as select 1");

        es.deployVDB("vdb", mmd1);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
        Statement s = c.createStatement();
        s.execute("create temporary table t (x serial, y string, z string, primary key (x))");
        PreparedStatement ps = c.prepareStatement("insert into t (y) values ('a')", Statement.RETURN_GENERATED_KEYS);
        assertFalse(ps.execute());
        assertEquals(1, ps.getUpdateCount());
        ResultSet rs = ps.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        //should just be the default, rather than an exception
        assertEquals(11, rs.getMetaData().getColumnDisplaySize(1));

        //test in a procedure without the statement directive
        ps = c.prepareStatement("begin insert into t (y) values ('b'); select cast(generated_key('x') as integer); end");
        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));

        //make sure the session scope works as well
        rs = s.executeQuery("select generated_key()");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
    }

    @Test public void testGeneratedKeyComposite() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);
        ec.setUseDisk(false);
        es.start(ec);

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.addSourceMetadata("ddl", "create view v as select 1");

        es.deployVDB("vdb", mmd1);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
        Statement s = c.createStatement();
        s.execute("create temporary table t (x string, y serial, z string, primary key (x, y))");
        PreparedStatement ps = c.prepareStatement("insert into t (x, z) values ('a', 'b')", Statement.RETURN_GENERATED_KEYS);
        assertFalse(ps.execute());
        assertEquals(1, ps.getUpdateCount());
        ResultSet rs = ps.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        //should just be the default, rather than an exception
        assertEquals(11, rs.getMetaData().getColumnDisplaySize(1));

        //test in a procedure without the statement directive
        ps = c.prepareStatement("begin insert into t (z) values ('b'); select generated_key(); end");
        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));

        //make sure the session scope works as well
        rs = s.executeQuery("select generated_key()");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
    }

    @Test public void testMultiSourceMetadata() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);

        ec.setUseDisk(false);
        es.start(ec);

        es.addTranslator("t", new ExecutionFactory<Void, Void>());

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.setSchemaSourceType("ddl");
        mmd1.setSchemaText("create foreign table t (x string)");
        mmd1.setSupportsMultiSourceBindings(true);
        mmd1.addSourceMapping("x", "t", null);
        mmd1.addSourceMapping("y", "t", null);

        es.deployVDB("vdb", mmd1);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
        PreparedStatement ps = c.prepareStatement("select * from t");
        ResultSetMetaData metadata = ps.getMetaData();
        assertEquals(1, metadata.getColumnCount());

        mmd1.addProperty("multisource.addColumn", Boolean.TRUE.toString());

        es.undeployVDB("vdb");
        es.deployVDB("vdb", mmd1);

        c = es.getDriver().connect("jdbc:teiid:vdb", null);
        ps = c.prepareStatement("select * from t");
        metadata = ps.getMetaData();
        assertEquals(2, metadata.getColumnCount());

        mmd1.addProperty("multisource.columnName", "y");

        es.undeployVDB("vdb");
        es.deployVDB("vdb", mmd1);

        c = es.getDriver().connect("jdbc:teiid:vdb", null);
        ps = c.prepareStatement("select * from t");
        metadata = ps.getMetaData();
        assertEquals(2, metadata.getColumnCount());
        assertEquals("y", metadata.getColumnName(2));
    }

    /**
     * Check that we'll consult each source
     * @throws Exception
     */
    @Test public void testMultiSourceMetadataMissingSource() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.start(ec);

        es.addTranslator("t", new ExecutionFactory<Object, Object>() {
            @Override
            public Object getConnection(Object factory) throws TranslatorException {
                return factory;
            }
            @Override
            public void closeConnection(Object connection, Object factory) {
            }
            @Override
            public void getMetadata(MetadataFactory metadataFactory, Object conn)
                    throws TranslatorException {
                assertNotNull(conn);
                Table t = metadataFactory.addTable("x");
                metadataFactory.addColumn("a", "string", t);
            }
        });
        es.addConnectionFactory("b", new Object());
        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.setSupportsMultiSourceBindings(true);
        mmd1.addSourceMapping("x", "t", "a"); //a is missing
        mmd1.addSourceMapping("y", "t", "b");

        es.deployVDB("vdb", mmd1);
    }

    @Test public void testDynamicUpdate() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);
        ec.setUseDisk(false);
        es.start(ec);

        es.addTranslator("t", new ExecutionFactory<Void, Void>() {

            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }

            @Override
            public boolean isSourceRequired() {
                return false;
            }

            @Override
            public UpdateExecution createUpdateExecution(Command command,
                    ExecutionContext executionContext,
                    RuntimeMetadata metadata, Void connection)
                    throws TranslatorException {
                Collection<Literal> values = CollectorVisitor.collectObjects(Literal.class, command);
                assertEquals(2, values.size());
                for (Literal literal : values) {
                    assertFalse(literal.getValue() instanceof Reference);
                }
                return new UpdateExecution() {

                    @Override
                    public void execute() throws TranslatorException {

                    }

                    @Override
                    public void close() {

                    }

                    @Override
                    public void cancel() throws TranslatorException {

                    }

                    @Override
                    public int[] getUpdateCounts() throws DataNotAvailableException,
                            TranslatorException {
                        return new int[] {1};
                    }
                };
            }
        });

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("accounts");
        mmd1.setSchemaSourceType("ddl");
        mmd1.setSchemaText(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("dynamic_update.sql")));
        mmd1.addSourceMapping("y", "t", null);

        es.deployVDB("vdb", mmd1);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
        PreparedStatement ps = c.prepareStatement("update hello1 set SchemaName=? where Name=?");
        ps.setString(1,"test1223");
        ps.setString(2,"Columns");
        assertEquals(1, ps.executeUpdate());
    }

    public static boolean started;

    public static class MyEF extends ExecutionFactory<Void, Void> {

        @Override
        public void start() throws TranslatorException {
            started = true;
        }
    }

    @Test public void testStart() throws TranslatorException {
        es.addTranslator(MyEF.class);
        assertTrue(started);
    }

    @Test public void testGlobalTempTables() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setMaxResultSetCacheStaleness(0);
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);
        ec.setUseDisk(false);
        es.start(ec);

        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE global temporary table some_temp (col1 string, col2 time) options (updatable true);]]> </metadata></model></vdb>".getBytes()));

        Connection c = es.getDriver().connect("jdbc:teiid:test", null);

        PreparedStatement ps = c.prepareStatement("/*+ cache */ select * from some_temp");
        ResultSet rs = ps.executeQuery();
        assertFalse(rs.next());

        Connection c1 = es.getDriver().connect("jdbc:teiid:test", null);
        c1.createStatement().execute("insert into some_temp (col1) values ('a')");

        PreparedStatement ps1 = c1.prepareStatement("/*+ cache */ select * from some_temp");
        ResultSet rs1 = ps1.executeQuery();
        assertTrue(rs1.next()); //there's a result for the second session

        rs = ps.executeQuery();
        assertFalse(rs.next()); //still no result in the first session

        c.createStatement().execute("insert into some_temp (col1) values ('b')");

        rs = ps.executeQuery();
        assertTrue(rs.next()); //still no result in the first session

        //ensure without caching that we have the right results
        rs = c.createStatement().executeQuery("select * from some_temp");
        assertTrue(rs.next());
        assertEquals("b", rs.getString(1));

        rs = c1.createStatement().executeQuery("select * from some_temp");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
    }

    @Test public void testMaxRows() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setMaxResultSetCacheStaleness(0);
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);
        ec.setUseDisk(false);
        es.start(ec);

        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE virtual procedure proc (out col1 string result) returns TABLE (r1 string) as begin col1 = 'a'; select 'b' union all select 'c'; end;]]> </metadata></model></vdb>".getBytes()));

        Connection c = es.getDriver().connect("jdbc:teiid:test", null);

        CallableStatement cs = c.prepareCall("{? = call proc()}");
        ResultSet rs = cs.executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
        assertEquals("a", cs.getString(1));

        //ensure that we don't drop the parameter row (which is last)
        cs.setMaxRows(1);
        rs = cs.executeQuery();
        assertTrue(rs.next());
        assertFalse(rs.next());
        assertEquals("a", cs.getString(1));

        //ensure that we can skip batches
        cs.setMaxRows(1);
        cs.setFetchSize(1);
        rs = cs.executeQuery();
        assertTrue(rs.next());
        assertFalse(rs.next());
        assertEquals("a", cs.getString(1));

        //cache should behave as expected when populated
        cs = c.prepareCall("/*+ cache */ {? = call proc()}");
        cs.setMaxRows(1);
        rs = cs.executeQuery();
        assertTrue(rs.next());
        assertFalse(rs.next());
        assertEquals("a", cs.getString(1));

        //accessing from cache without the max should still give us the full result
        cs.setMaxRows(0);
        rs = cs.executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
        assertEquals("a", cs.getString(1));

        //accessing again with max should give the smaller result
        cs.setMaxRows(1);
        rs = cs.executeQuery();
        assertTrue(rs.next());
        assertFalse(rs.next());
        assertEquals("a", cs.getString(1));
    }

    @Test public void testSourceLobUnderTxn() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setMaxResultSetCacheStaleness(0);
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);
        ec.setUseDisk(false);
        es.start(ec);

        final AtomicBoolean closed = new AtomicBoolean();
        es.addTranslator("foo", new ExecutionFactory() {

            @Override
            public boolean isSourceRequired() {
                return false;
            }

            @Override
            public ResultSetExecution createResultSetExecution(
                    QueryExpression command, ExecutionContext executionContext,
                    RuntimeMetadata metadata, Object connection)
                    throws TranslatorException {
                return new ResultSetExecution() {

                    private boolean returned;

                    @Override
                    public void execute() throws TranslatorException {

                    }

                    @Override
                    public void close() {
                        closed.set(true);
                    }

                    @Override
                    public void cancel() throws TranslatorException {

                    }

                    @Override
                    public List<?> next() throws TranslatorException, DataNotAvailableException {
                        if (returned) {
                            return null;
                        }
                        returned = true;
                        ArrayList<Object> result = new ArrayList<Object>(1);
                        result.add(new SQLXMLImpl(new InputStreamFactory() {

                            @Override
                            public InputStream getInputStream() throws IOException {
                                //need to make it of a sufficient size to not be inlined
                                return new ByteArrayInputStream(new byte[DataTypeManager.MAX_LOB_MEMORY_BYTES + 1]);
                            }
                        }));
                        return result;
                    }
                };
            }
        });
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\"><source name=\"foo\" translator-name=\"foo\"/><metadata type=\"DDL\"><![CDATA[CREATE foreign table x (y xml);]]> </metadata></model></vdb>".getBytes()));

        Connection c = es.getDriver().connect("jdbc:teiid:test", null);

        c.setAutoCommit(false);

        Statement s = c.createStatement();

        ResultSet rs = s.executeQuery("select * from x");

        rs.next();

        assertFalse(closed.get());

        s.close();

        assertTrue(closed.get());
    }

    @Test public void testUndeploy() throws Exception {
        es.start(new EmbeddedConfiguration());
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model type=\"VIRTUAL\" name=\"test\"><metadata type=\"DDL\"><![CDATA[CREATE view x as select 1;]]> </metadata></model></vdb>".getBytes()));
        Connection c = es.getDriver().connect("jdbc:teiid:test", null);
        assertTrue(c.isValid(10));
        es.undeployVDB("test");
        assertTrue(!c.isValid(10));
    }

    @Test public void testAsyncDeploy() throws Exception {
        es.start(new EmbeddedConfiguration());
        CompletableFuture<org.teiid.adminapi.VDB.Status> future = new CompletableFuture<>();
        Thread t = Thread.currentThread();
        es.getVDBRepository().addListener(new VDBLifeCycleListener() {
            @Override
            public void finishedDeployment(String name, CompositeVDB vdb) {
                if (Thread.currentThread() == t) {
                    future.completeExceptionally(new AssertionError("Same thread"));
                } else {
                    future.complete(vdb.getVDB().getStatus());
                }
            }
        });
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><property name=\"async-load\" value=\"true\"></property><model type=\"VIRTUAL\" name=\"test\"><metadata type=\"DDL\"><![CDATA[CREATE view x as select 1;]]> </metadata></model></vdb>".getBytes()));
        assertEquals(org.teiid.adminapi.VDB.Status.ACTIVE, future.get(5, TimeUnit.SECONDS));
    }

    @Test public void testAsyncDeployError() throws Exception {
        es.start(new EmbeddedConfiguration());
        CompletableFuture<org.teiid.adminapi.VDB.Status> future = new CompletableFuture<>();
        Thread t = Thread.currentThread();
        es.getVDBRepository().addListener(new VDBLifeCycleListener() {
            @Override
            public void finishedDeployment(String name, CompositeVDB vdb) {
                if (Thread.currentThread() == t) {
                    future.completeExceptionally(new AssertionError("Same thread"));
                } else {
                    future.complete(vdb.getVDB().getStatus());
                }
            }
        });
        es.addMetadataRepository("CUSTOM", new MetadataRepository<Object, Object>() {

            @Override
            public void loadMetadata(MetadataFactory factory,
                    ExecutionFactory<Object, Object> executionFactory,
                    Object connectionFactory, String text)
                    throws TranslatorException {
                throw new TranslatorException();
            }

        });
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><property name=\"async-load\" value=\"true\"></property><model type=\"VIRTUAL\" name=\"test\"><metadata type=\"CUSTOM\"><![CDATA[CREATE view x as select 1;]]> </metadata></model></vdb>".getBytes()));
        assertEquals(org.teiid.adminapi.VDB.Status.FAILED, future.get(5, TimeUnit.SECONDS));
    }

    @Test public void testQueryTimeout() throws Exception {
        es.start(new EmbeddedConfiguration());
        es.addTranslator("foo", new ExecutionFactory() {
            @Override
            public boolean isSourceRequired() {
                return false;
            }

            @Override
            public ResultSetExecution createResultSetExecution(
                    QueryExpression command, ExecutionContext executionContext,
                    RuntimeMetadata metadata, Object connection)
                    throws TranslatorException {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                }
                return super.createResultSetExecution(command, executionContext, metadata,
                        connection);
            }

        });
        es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\"><source name=\"foo\" translator-name=\"foo\"/><metadata type=\"DDL\"><![CDATA[CREATE foreign table x (y xml);]]> </metadata></model></vdb>".getBytes()));
        Connection c = es.getDriver().connect("jdbc:teiid:test", null);
        Statement s = c.createStatement();
        s.setQueryTimeout(1);
        try {
            s.execute("select * from x");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLStates.QUERY_CANCELED, e.getSQLState());
        }

    }

    @Test
    public void testMultipleEmbeddedServerInOneVM() throws TranslatorException {
        es.addTranslator(MyEFES1.class);
        es.start(new EmbeddedConfiguration());
        assertTrue(isES1started);

        EmbeddedServer es2 = new EmbeddedServer();
        es2.start(new EmbeddedConfiguration());
        es2.addTranslator(MyEFES2.class);
        assertTrue(isES2started);
        es2.stop();
    }

    public static boolean isES1started;
    public static boolean isES2started;

    public static class MyEFES1 extends ExecutionFactory<Void, Void> {

        @Override
        public void start() throws TranslatorException {
            isES1started = true;
        }
    }

    public static class MyEFES2 extends ExecutionFactory<Void, Void> {

        @Override
        public void start() throws TranslatorException {
            isES2started = true;
        }
    }

    @Test
    public void testExternalMaterializationManagement() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        ec.setTransactionManager(SimpleMock.createSimpleMock(TransactionManager.class));
        es.transactionService.setXaImporter(SimpleMock.createSimpleMock(XAImporter.class));

        es.start(ec);
        es.transactionService.setDetectTransactions(false);

        final AtomicBoolean loaded = new AtomicBoolean();
        final AtomicBoolean valid = new AtomicBoolean();
        final AtomicInteger matTableCount = new AtomicInteger();
        final AtomicInteger tableCount = new AtomicInteger();
        final AtomicBoolean hasStatus = new AtomicBoolean();

        es.addTranslator("y", new ExecutionFactory<AtomicInteger, Object> () {
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }

            @Override
            public Object getConnection(AtomicInteger factory)
                    throws TranslatorException {
                return factory.incrementAndGet();
            }

            @Override
            public void closeConnection(Object connection, AtomicInteger factory) {

            }

            @Override
            public void getMetadata(MetadataFactory metadataFactory, Object conn)
                    throws TranslatorException {
                assertEquals(conn, Integer.valueOf(1));
                Table t = metadataFactory.addTable("my_table");
                t.setSupportsUpdate(true);
                Column c = metadataFactory.addColumn("my_column", TypeFacility.RUNTIME_NAMES.STRING, t);
                c.setUpdatable(true);

                // mat table
                t = metadataFactory.addTable("mat_table");
                t.setSupportsUpdate(true);
                c = metadataFactory.addColumn("my_column", TypeFacility.RUNTIME_NAMES.STRING, t);
                c.setUpdatable(true);

                // status table
                t = metadataFactory.addTable("status");
                t.setSupportsUpdate(true);
                c = metadataFactory.addColumn("VDBName", TypeFacility.RUNTIME_NAMES.STRING, t);
                c.setUpdatable(true);
                c = metadataFactory.addColumn("VDBVersion", TypeFacility.RUNTIME_NAMES.STRING, t);
                c.setUpdatable(true);
                c = metadataFactory.addColumn("SchemaName", TypeFacility.RUNTIME_NAMES.STRING, t);
                c.setUpdatable(true);
                c = metadataFactory.addColumn("Name", TypeFacility.RUNTIME_NAMES.STRING, t);
                c.setUpdatable(true);
                c = metadataFactory.addColumn("TargetSchemaName", TypeFacility.RUNTIME_NAMES.STRING, t);
                c.setUpdatable(true);
                c = metadataFactory.addColumn("TargetName", TypeFacility.RUNTIME_NAMES.STRING, t);
                c.setUpdatable(true);
                c = metadataFactory.addColumn("Valid", TypeFacility.RUNTIME_NAMES.BOOLEAN, t);
                c.setUpdatable(true);
                c = metadataFactory.addColumn("LoadState", TypeFacility.RUNTIME_NAMES.STRING, t);
                c.setUpdatable(true);
                c = metadataFactory.addColumn("Cardinality", TypeFacility.RUNTIME_NAMES.LONG, t);
                c.setUpdatable(true);
                c = metadataFactory.addColumn("Updated", TypeFacility.RUNTIME_NAMES.TIMESTAMP, t);
                c.setUpdatable(true);
                c = metadataFactory.addColumn("LoadNumber", TypeFacility.RUNTIME_NAMES.LONG, t);
                c.setUpdatable(true);
                c = metadataFactory.addColumn("NodeName", TypeFacility.RUNTIME_NAMES.STRING, t);
                c.setUpdatable(true);
                c = metadataFactory.addColumn("StaleCount", TypeFacility.RUNTIME_NAMES.LONG, t);
                c.setUpdatable(true);
                metadataFactory.addPrimaryKey("PK", Arrays.asList("VDBName", "VDBVersion", "SchemaName", "Name"), t);
            }

            @Override
            public ResultSetExecution createResultSetExecution(
                final QueryExpression command, final ExecutionContext executionContext,
                final RuntimeMetadata metadata, final Object connection)
                throws TranslatorException {

                return new ResultSetExecution() {
                    Iterator<? extends List<? extends Object>> results;
                    @Override
                    public void execute() throws TranslatorException {
                    }
                    @Override
                    public void close() {
                    }
                    @Override
                    public void cancel() throws TranslatorException {
                    }
                    @Override
                    public List<?> next() throws TranslatorException, DataNotAvailableException {
                        if (results == null) {
                            String commandString = command.toString();
                            if (commandString.contains(" FROM status")) {
                                if (hasStatus.get()) {
                                    if (commandString.startsWith("SELECT status.Valid, status.LoadState FROM status")) {
                                        results = Arrays.asList(Arrays.asList(valid.get(), loaded.get()?"LOADED":"LOADING")).iterator();
                                    } else if (commandString.startsWith("SELECT status.Name, status.TargetSchemaName, status.TargetName, status.Valid, status.LoadState, status.Updated, status.Cardinality, status.LoadNumber")) {
                                        results = Arrays.asList(Arrays.asList("my_view", "my_schema", "mat_table", valid.get(), loaded.get()?"LOADED":"LOADING", new Timestamp(System.currentTimeMillis()), -1, new Integer(1))).iterator();
                                    } else {
                                        throw new AssertionError(commandString);
                                    }
                                }
                            } else if (loaded.get() && commandString.equals("SELECT mat_table.my_column FROM mat_table")) {
                                matTableCount.getAndIncrement();
                                results = Arrays.asList(Arrays.asList("mat_column0"), Arrays.asList("mat_column1")).iterator();
                            } else if (commandString.equals("SELECT my_table.my_column FROM my_table")) {
                                tableCount.getAndIncrement();
                                results = Arrays.asList(Arrays.asList("regular_column")).iterator();
                            }
                        }
                        if (results != null && results.hasNext()) {
                            return results.next();
                        }
                        return null;
                    }
                };
            }

            @Override
            public UpdateExecution createUpdateExecution(final Command command,
                    final ExecutionContext executionContext,
                    final RuntimeMetadata metadata, final Object connection)
                    throws TranslatorException {
                UpdateExecution ue = new UpdateExecution() {

                    @Override
                    public void execute() throws TranslatorException {
                        String commandString = command.toString();
                        if (commandString.startsWith("INSERT INTO status")) {
                            hasStatus.set(true);
                        }
                        if (commandString.startsWith("INSERT INTO status") || commandString.startsWith("UPDATE status SET")) {
                            if (commandString.contains("LoadState")) {
                                synchronized (loaded) {
                                    loaded.set(commandString.indexOf("LOADED") != -1);
                                    loaded.notifyAll();
                                }
                            }
                            if (commandString.contains("Valid")) {
                                valid.set(commandString.indexOf("TRUE") != -1);
                            }
                        }
                    }

                    @Override
                    public void close() {

                    }

                    @Override
                    public void cancel() throws TranslatorException {

                    }

                    @Override
                    public int[] getUpdateCounts() throws DataNotAvailableException,
                            TranslatorException {
                        return new int[] {1};
                    }
                };
                return ue;
            }
        });
        final AtomicInteger counter = new AtomicInteger();
        ConnectionFactoryProvider<AtomicInteger> cfp = new EmbeddedServer.SimpleConnectionFactoryProvider<AtomicInteger>(counter);

        es.addConnectionFactoryProvider("z", cfp);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("my_schema");
        mmd.addSourceMapping("x", "y", "z");

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("virt");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.setSchemaSourceType("ddl");
        mmd1.setSchemaText("create view my_view OPTIONS (" +
                "UPDATABLE 'true',MATERIALIZED 'TRUE',\n" +
                "MATERIALIZED_TABLE 'my_schema.mat_table', \n" +
                "\"teiid_rel:MATERIALIZED_STAGE_TABLE\" 'my_schema.mat_table',\n" +
                "\"teiid_rel:ALLOW_MATVIEW_MANAGEMENT\" 'true', \n" +
                "\"teiid_rel:MATVIEW_STATUS_TABLE\" 'my_schema.status', \n" +
                "\"teiid_rel:MATVIEW_AFTER_LOAD_SCRIPT\" 'select 1; select 1, ''a''', \n" +
                "\"teiid_rel:MATVIEW_SHARE_SCOPE\" 'FULL',\n" +
                "\"teiid_rel:MATVIEW_ONERROR_ACTION\" 'THROW_EXCEPTION',\n" +
                "\"teiid_rel:MATVIEW_TTL\" 100000)" +
                "as select * from \"my_table\";"
                + " create view mat_table as select 'I conflict';");

        es.deployVDB("test", mmd, mmd1);
        synchronized (loaded) {
            while (!loaded.get()) {
                loaded.wait();
            }
        }
        Thread.sleep(2000); //need to ensure that the mat view is built

        final TeiidDriver td = es.getDriver();
        Connection c = td.connect("jdbc:teiid:test", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from my_view");
        assertTrue(rs.next());
        assertEquals("mat_column0", rs.getString(1));

        s.execute("update my_schema.status set valid=false");

        try {
            rs = s.executeQuery("select * from my_view");
            fail("expected throw exception to work");
        } catch (SQLException e) {

        }

        assertEquals(1, tableCount.get());

        s.execute("update my_schema.status set valid=true");

        //make sure a similar name doesn't cause an issue
          rs = s.executeQuery("select * from (call sysadmin.updateMatView('virt', 'my_view', 'true')) as x");
          rs.next();
          assertEquals(2, rs.getInt(1));

        assertEquals(2, tableCount.get());

        s.execute("call setProperty((SELECT UID FROM Sys.Tables WHERE SchemaName = 'virt' AND Name = 'my_view'), 'teiid_rel:MATVIEW_ONERROR_ACTION', 'WAIT')");

        //this thread should hang, until the status changes
        final AtomicBoolean success = new AtomicBoolean();
        Thread t = new Thread() {
            public void run() {
                try {
                    Connection c1 = td.connect("jdbc:teiid:test", null);
                    Statement s1 = c1.createStatement();
                    s1.executeQuery("select * from my_view");
                    success.set(true);
                } catch (SQLException e) {
                }
            };
        };
        t.start();

        //wait to ensure that the thread is blocked
        Thread.sleep(5000);

        //update the status and make sure the thread finished
        s.execute("update my_schema.status set valid=true");
        t.join(10000);
        assertTrue(success.get());
    }

    @Test public void testPreparedTypeResolving() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);
        ec.setUseDisk(false);
        es.start(ec);

        es.addTranslator("t", new ExecutionFactory<Void, Void>());

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.setSchemaSourceType("ddl");
        mmd1.setSchemaText("create view v (i integer) as select 1");

        es.deployVDB("vdb", mmd1);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
        //should expect a clob
        PreparedStatement ps = c.prepareStatement("select * from texttable(? columns a string) as x");
        ps.setCharacterStream(1, new StringReader("a\nb"));
        assertTrue(ps.execute());
        ResultSet rs = ps.getResultSet();
        rs.next();
        assertEquals("a", rs.getString(1));
    }

   @Test public void testPreparedLobUsage() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.start(ec);

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.addSourceMetadata("ddl", "create view v (i integer) as select 1; "
                + "CREATE VIRTUAL function clobfunction(p1 clob) RETURNS clob as return concat(p1, p1);"
                + "CREATE VIRTUAL function blobfunction(p1 blob) RETURNS clob as return concat(to_chars(p1, 'ascii'), to_chars(p1, 'utf-8'));");

        es.deployVDB("vdb", mmd1);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
        //should expect a clob
        PreparedStatement ps = c.prepareStatement("select clobfunction(?), blobfunction(?)");
        ps.setCharacterStream(1, new StringReader("abc"));
        ps.setBinaryStream(2, new ByteArrayInputStream("cba".getBytes("UTF-8")));
        assertTrue(ps.execute());
        ResultSet rs = ps.getResultSet();
        rs.next();
        assertEquals("abcabc", rs.getString(1));
        assertEquals("cbacba", rs.getString(2));
    }

    @Test public void testGeometrySelect() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.start(ec);

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.setSchemaSourceType("ddl");
        mmd1.setSchemaText("create view v (i geometry) as select ST_GeomFromText('POLYGON ((100 100, 200 200, 75 75, 100 100))')");

        es.deployVDB("vdb", mmd1);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select * from v");
        assertEquals("geometry", rs.getMetaData().getColumnTypeName(1));
        assertEquals(Types.BLOB, rs.getMetaData().getColumnType(1));
        assertEquals("java.sql.Blob", rs.getMetaData().getColumnClassName(1));
        rs.next();
        assertEquals(77, rs.getBlob(1).length());
    }

    @Test public void testUpdateCountAnonProc() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.start(ec);

        ModelMetaData mmd1 = new ModelMetaData();
        mmd1.setName("b");
        mmd1.setModelType(Type.VIRTUAL);
        mmd1.addSourceMetadata("ddl", "create view v as select 1");

        es.deployVDB("vdb", mmd1);

        Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
        Statement s = c.createStatement();
        s.execute("set autoCommitTxn off");
        PreparedStatement ps = c.prepareStatement("begin select 1 without return; end");
        assertNull(ps.getMetaData());
        ps.execute();
        assertEquals(0, ps.getUpdateCount());
        assertNull(ps.getMetaData());
    }

    public static class MyPreParser implements PreParser {

        @Override
        public String preParse(String command, CommandContext context) {
            if (command.equals("select 'goodbye'")) {
                return "select 'vdb'";
            }
            return command;
        }

    }

    @Test public void testPreParser() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setPreParser(new PreParser() {

            @Override
            public String preParse(String command, CommandContext context) {
                if (command.equals("select 'hello world'")) {
                    return "select 'goodbye'";
                }
                return command;
            }
        });
        es.start(ec);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("y");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create view dummy as select 1;");
        es.deployVDB("x", mmd);

        Connection c = es.getDriver().connect("jdbc:teiid:x", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select 'hello world'");
        rs.next();
        assertEquals("goodbye", rs.getString(1));

        String perVdb = "<vdb name=\"x1\" version=\"1\"><property name=\"preparser-class\" value=\""+MyPreParser.class.getName()+"\"/><model name=\"x\" type=\"VIRTUAL\"><metadata type=\"ddl\">create view v as select 1</metadata></model></vdb>";

        es.deployVDB(new ByteArrayInputStream(perVdb.getBytes("UTF-8")));
        c = es.getDriver().connect("jdbc:teiid:x1", null);
        s = c.createStatement();
        rs = s.executeQuery("select 'hello world'");
        rs.next();
        assertEquals("vdb", rs.getString(1));
    }

    @Test public void testTurnOffLobCleaning() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("y");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create view dummy as select 1;");
        es.deployVDB("x", mmd);

        Connection c = es.getDriver().connect("jdbc:teiid:x", null);
        Statement s = c.createStatement();
        s.execute("select teiid_session_set('clean_lobs_onclose', false)");
        ResultSet rs = s.executeQuery("WITH t(n) AS ( VALUES (1) UNION ALL SELECT n+1 FROM t WHERE n < 5000 ) SELECT xmlelement(root, xmlagg(xmlelement(val, n))) FROM t");
        assertTrue(rs.next());
        SQLXML val = rs.getSQLXML(1);
        rs.close();
        assertEquals(73906, val.getString().length());
    }

    @Test public void testDefaultEscape() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("y");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create view dummy as select 1;");
        es.deployVDB("x", mmd);

        Connection c = es.getDriver().connect("jdbc:teiid:x", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select '_a' like '\\_a'");
        rs.next();
        assertFalse(rs.getBoolean(1));
        s.execute("select teiid_session_set('backslashDefaultMatchEscape', true)");
        rs = s.executeQuery("select '_a' like '\\_a'");
        rs.next();
        assertTrue(rs.getBoolean(1));
    }

    @Test
    public void testBufferManagerProperties() throws TranslatorException {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();

        ec.setUseDisk(true);
        ec.setBufferDirectory(System.getProperty("java.io.tmpdir"));
        ec.setProcessorBatchSize(BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE);
        ec.setMaxReserveKb(BufferManager.DEFAULT_RESERVE_BUFFER_KB);
        ec.setMaxProcessingKb(BufferManager.DEFAULT_MAX_PROCESSING_KB);

        ec.setInlineLobs(true);
        ec.setMaxOpenFiles(FileStorageManager.DEFAULT_MAX_OPEN_FILES);
        ec.setMaxBufferSpace(FileStorageManager.DEFAULT_MAX_BUFFERSPACE>>20);
        ec.setMaxFileSize(SplittableStorageManager.DEFAULT_MAX_FILESIZE);
        ec.setEncryptFiles(false);

        ec.setMaxStorageObjectSize(BufferFrontedFileStoreCache.DEFAULT_MAX_OBJECT_SIZE);
        ec.setMemoryBufferOffHeap(false);

        started = false;
        es.addTranslator(MyEF.class);
        es.start(ec);
        assertTrue(started);
    }

    @Test(expected=VirtualDatabaseException.class)
    public void testRequireRoles() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        es.repo.setDataRolesRequired(true);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("y");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create view dummy as select 1;");
        es.deployVDB("x", mmd);
    }

    @Test public void testSystemSubquery() throws Exception {
        //already working
        String query = "SELECT t.Name FROM (select * from SYS.Tables AS t limit 10) as t, (SELECT DISTINCT c.TableName FROM SYS.Columns AS c where c.Name > 'A') AS X__1 WHERE t.Name = X__1.TableName";
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create view v as select 1 as c");
        es.deployVDB("x", mmd);
        Connection c = es.getDriver().connect("jdbc:teiid:x", null);
        Statement s = c.createStatement();
        extractRowCount(query, s, 10);

        //was throwing exception
        query = "SELECT t.Name FROM (select * from SYS.Tables AS t limit 10) as t, (SELECT DISTINCT c.TableName FROM SYS.Columns AS c) AS X__1 WHERE t.Name = X__1.TableName";
        extractRowCount(query, s, 10);
    }

    private void extractRowCount(String query, Statement s, int count)
            throws SQLException {
        s.execute(query);
        ResultSet rs = s.getResultSet();
        int i = 0;
        while (rs.next()) {
            i++;
        }
        assertEquals(count, i);
    }

    @Test public void testTempVisibilityToExecuteImmediate() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create procedure p () as execute immediate 'select 1' as x integer into #temp;");
        es.deployVDB("x", mmd);
        Connection c = es.getDriver().connect("jdbc:teiid:x", null);
        Statement s = c.createStatement();
        s.execute("create local temporary table #temp (x integer)");
        s.execute("exec p()");
        extractRowCount("select * from #temp", s, 0);
    }

    @Test public void testSubqueryCache() throws Exception {
        UnitTestUtil.enableLogging(Level.WARNING, "org.teiid");
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("proc.sql")));
        es.deployVDB("x", mmd);
        Connection c = es.getDriver().connect("jdbc:teiid:x", null);
        Statement s = c.createStatement();
        String[] statements = ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("data.sql")).split(";");
        for (String statement:statements) {
            s.execute(statement);
        }
        s.execute("set autoCommitTxn off");
        s.execute("SELECT \"citmp.KoordX\" ,\"citmp.KoordY\" ,( SELECT \"store.insideFence\" FROM ( EXEC point_inside_store ( CAST ( \"citmp.KoordX\" AS float ) ,CAST ( \"citmp.KoordY\" AS float ) ) ) as \"store\" ) as \"insideStore\" ,( SELECT \"firstsection.insideFence\" FROM ( EXEC point_inside_store ( CAST ( \"citmp.KoordX\" AS float ) ,CAST ( \"citmp.KoordY\" AS float ) ) ) as \"firstsection\" ) as \"insideFirstsection\" FROM sample_coords as \"citmp\" ORDER BY insideStore ASC ,insideFirstsection DESC");
        ResultSet rs = s.getResultSet();
        while (rs.next()) {
            //the last two columns should be identical
            assertEquals(rs.getInt(3), rs.getInt(4));
        }
    }

    @Test(expected=TeiidSQLException.class) public void testCancelSystemQuery() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create view v as select 1;");
        es.deployVDB("x", mmd);
        Connection c = es.getDriver().connect("jdbc:teiid:x", null);
        final Statement s = c.createStatement();
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                    s.cancel();
                } catch (InterruptedException e) {
                } catch (SQLException e) {
                }
            }
        };
        t.start();
        ResultSet rs = s.executeQuery("select count(*) from columns c, columns c1, columns c2, columns c3, columns c4");
        rs.next();
        fail();
    }

    @Test public void testSemanticVersioning() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("x");
        mmd.setModelType(Type.VIRTUAL);
        mmd.addSourceMetadata("ddl", "create view v as select 1;");

        es.deployVDB("x.0.9.0", mmd);
        es.deployVDB("x.1.0.1", mmd);
        es.deployVDB("x.1.1.0", mmd);

        Connection c = es.getDriver().connect("jdbc:teiid:x", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("values (current_database())");
        rs.next();
        assertEquals("x", rs.getString(1));

        rs = s.executeQuery("select version from virtualdatabases");
        rs.next();
        assertEquals("0.9.0", rs.getString(1));

        try {
            //v1.0.0 does not exist
            c = es.getDriver().connect("jdbc:teiid:x.v1.0", null);
            fail();
        } catch (TeiidSQLException e) {

        }

        c = es.getDriver().connect("jdbc:teiid:x.1.0.1", null);
        s = c.createStatement();
        rs = s.executeQuery("select version from virtualdatabases");
        rs.next();
        assertEquals("1.0.1", rs.getString(1));

        try {
            //old style non-semantic version
            c = es.getDriver().connect("jdbc:teiid:x.1", null);
            fail();
        } catch (TeiidSQLException e) {

        }

        c = es.getDriver().connect("jdbc:teiid:x.1.", null);
        s = c.createStatement();
        rs = s.executeQuery("select version from virtualdatabases");
        rs.next();
        assertEquals("1.0.1", rs.getString(1));


        c = es.getDriver().connect("jdbc:teiid:x.1.1.", null);
        s = c.createStatement();
        rs = s.executeQuery("select version from virtualdatabases");
        rs.next();
        assertEquals("1.1.0", rs.getString(1));
    }

    @Test public void testSemanticVersioningAny() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);

        es.deployVDB(new ByteArrayInputStream(createVDB("x", "0.9").getBytes("UTF-8")));

        Connection c = es.getDriver().connect("jdbc:teiid:x", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("values (current_database())");
        rs.next();
        assertEquals("x", rs.getString(1));

        rs = s.executeQuery("select version from virtualdatabases");
        rs.next();
        assertEquals("0.9.0", rs.getString(1));

        es.deployVDB(new ByteArrayInputStream(createVDB("x", "1.1.1").getBytes("UTF-8")));

        try {
            //old style non-semantic version
            c = es.getDriver().connect("jdbc:teiid:x.1", null);
            fail();
        } catch (TeiidSQLException e) {

        }

        c = es.getDriver().connect("jdbc:teiid:x.1.", null);
        s = c.createStatement();
        rs = s.executeQuery("select version from virtualdatabases");
        rs.next();
        assertEquals("1.1.1", rs.getString(1));

        c = es.getDriver().connect("jdbc:teiid:x.1.1.", null);
        s = c.createStatement();
        rs = s.executeQuery("select version from virtualdatabases");
        rs.next();
        assertEquals("1.1.1", rs.getString(1));

        es.deployVDB(new ByteArrayInputStream(createVDB("x", "1.11.1").getBytes("UTF-8")));
        es.deployVDB(new ByteArrayInputStream(createVDB("x", "1.0.1").getBytes("UTF-8")));

        c = es.getDriver().connect("jdbc:teiid:x.1.1.", null);
        s = c.createStatement();
        rs = s.executeQuery("select version from virtualdatabases");
        rs.next();
        assertEquals("1.1.1", rs.getString(1));

        c = es.getDriver().connect("jdbc:teiid:x.1.0.", null);
        s = c.createStatement();
        rs = s.executeQuery("select version from virtualdatabases");
        rs.next();
        assertEquals("1.0.1", rs.getString(1));

        try {
            c = es.getDriver().connect("jdbc:teiid:x.1.12.0", null);
            fail();
        } catch (TeiidSQLException e) {

        }
    }

    private String createVDB(String name, String version) {
        return "<vdb name=\""+ name +"\" version=\""+version+"\"><connection-type>ANY</connection-type><model name=\"x\" type=\"VIRTUAL\"><metadata type=\"ddl\">create view v as select 1</metadata></model></vdb>";
    }

    @Test public void testVirtualFunctions() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        es.deployVDB(new ByteArrayInputStream(("<vdb name=\"ddlfunctions\" version=\"1\">"
                + "<model visible=\"true\" type=\"VIRTUAL\" name=\"FunctionModel\">"
                + "         <metadata type=\"DDL\"><![CDATA["
                + "        CREATE VIRTUAL function f1(p1 integer) RETURNS integer as return p1;"
                + "        CREATE VIEW TestView (c1 integer) AS SELECT f1(42) AS c1;"
                + "       ]]>"
                + "</metadata></model></vdb>").getBytes()));
        Connection c = es.getDriver().connect("jdbc:teiid:ddlfunctions", null);
        Statement s = c.createStatement();
        s.execute("select f1(1)");
        ResultSet rs = s.getResultSet();
        rs.next();
        assertEquals(1, rs.getInt(1));
        s.execute("select * from testview");
        rs = s.getResultSet();
        rs.next();
        assertEquals(42, rs.getInt(1));
    }

    @Test public void testWithPushdownChangeName() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.start(ec);

        HardCodedExecutionFactory hcef = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCommonTableExpressions() {
                return true;
            }

            @Override
            public boolean supportsSelfJoins() {
                return true;
            }

            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }

            @Override
            public boolean supportsAliasedTable() {
                return true;
            }

            @Override
            public boolean isSourceRequired() {
                return false;
            }

            @Override
            public String getExcludedCommonTableExpressionName() {
                return "a";
            }

            @Override
            public boolean supportsInnerJoins() {
                return true;
            }
        };

        es.addTranslator("y", hcef);

        hcef.addData("WITH a__2 (x) AS (SELECT g_0.e1 FROM pm1.g1 AS g_0) SELECT g_0.x FROM a__2 AS g_0, a__2 AS g_1", Arrays.asList(Arrays.asList("a")));

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("my-schema");
        mmd.addSourceMapping("x", "y", null);
        mmd.addSourceMetadata("ddl", "create foreign table \"pm1.g1\" (e1 string)");

        es.deployVDB("test", mmd);

        TeiidDriver td = es.getDriver();
        Connection c = td.connect("jdbc:teiid:test", null);
        Statement s = c.createStatement();

        //see the correct pushdown in hcef.addData above
        s.execute("with a (x) as (select e1 from pm1.g1) SELECT a.x from a, a z"); //$NON-NLS-1$
    }

    @Test public void testBatchedUpdateErrors() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        MockTransactionManager tm = new MockTransactionManager();
        ec.setTransactionManager(tm);

        ec.setUseDisk(false);
        es.start(ec);

        HardCodedExecutionFactory hcef = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }
        };
        hcef.addUpdate("UPDATE pm1.g1 SET e1 = 'a' WHERE pm1.g1.e2 = 1", new int[] {1});
        hcef.addUpdate("UPDATE pm1.g1 SET e1 = 'b' WHERE pm1.g1.e2 = 2", new TranslatorException("i've failed"));
        es.addTranslator("y", hcef);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("my-schema");
        mmd.addSourceMapping("x", "y", null);
        mmd.addSourceMetadata("ddl", "create foreign table \"pm1.g1\" (e1 string, e2 integer) options (updatable true)");

        es.deployVDB("test", mmd);

        TeiidDriver td = es.getDriver();
        Connection c = td.connect("jdbc:teiid:test", null);
        Statement s = c.createStatement();

        s.addBatch("update pm1.g1 set e1 = 'a' where e2 = 1"); //$NON-NLS-1$
        s.addBatch("update pm1.g1 set e1 = 'b' where e2 = 2"); //$NON-NLS-1$
        try {
            s.executeBatch();
            fail();
        } catch (BatchUpdateException e) {
            int[] updateCounts = e.getUpdateCounts();
            assertArrayEquals(new int[] {1}, updateCounts);
            assertEquals(-1, s.getUpdateCount());
        }

        //redeploy with batch support

        hcef = new HardCodedExecutionFactory() {
            @Override
            public boolean supportsCompareCriteriaEquals() {
                return true;
            }

            @Override
            public boolean supportsBatchedUpdates() {
                return true;
            }
        };

        es.addTranslator("z", hcef);
        es.undeployVDB("test");
        mmd = new ModelMetaData();
        mmd.setName("my-schema");
        mmd.addSourceMetadata("ddl", "create foreign table \"pm1.g1\" (e1 string, e2 integer) options (updatable true)");
        mmd.addSourceMapping("y", "z", null);
        es.deployVDB("test", mmd);

        c = td.connect("jdbc:teiid:test", null);
        s = c.createStatement();

        s.addBatch("update pm1.g1 set e1 = 'a' where e2 = 1"); //$NON-NLS-1$
        s.addBatch("update pm1.g1 set e1 = 'b' where e2 = 2"); //$NON-NLS-1$
        hcef.updateMap.clear();
        hcef.addUpdate("UPDATE pm1.g1 SET e1 = 'a' WHERE pm1.g1.e2 = 1;\nUPDATE pm1.g1 SET e1 = 'b' WHERE pm1.g1.e2 = 2;", new TranslatorBatchException(new SQLException(), new int[] {1, -3}));
        try {
            s.executeBatch();
            fail();
        } catch (BatchUpdateException e) {
            int[] updateCounts = e.getUpdateCounts();
            assertArrayEquals(new int[] {1, -3}, updateCounts);
            assertEquals(-1, s.getUpdateCount());
        }
    }

    @Translator(name="dummy")
    public static class DummyExecutionFactory extends ExecutionFactory {

        static AtomicInteger INSTANCES = new AtomicInteger();

        int instance = INSTANCES.getAndIncrement();

        @Override
        public void getMetadata(MetadataFactory metadataFactory, Object conn)
                throws TranslatorException {
            Table t = metadataFactory.addTable("test"+ String.valueOf(instance));
            if (this.supportsOrderBy()) {
                metadataFactory.addColumn("y", "integer", t);
            } else {
                metadataFactory.addColumn("x", "integer", t);
            }
        }

        @Override
        public boolean isSourceRequiredForMetadata() {
            return false;
        }

    };

    @Test public void testTranslatorCreation() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setUseDisk(false);
        es.start(ec);

        es.addTranslator(DummyExecutionFactory.class);

        Map<String, String> props = new HashMap<String, String>();
        props.put("supportsOrderBy", "true");

        es.addTranslator("dummy-override", "dummy", props);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("test-one");
        mmd.addSourceMapping("one", "dummy", null);

        ModelMetaData mmd2 = new ModelMetaData();
        mmd2.setName("test-two");
        mmd2.addSourceMapping("two", "dummy", null);

        ModelMetaData mmd3 = new ModelMetaData();
        mmd3.setName("test-three");
        mmd3.addSourceMapping("three", "dummy-override", null);

        es.deployVDB("test", mmd, mmd2, mmd3);

        TeiidDriver td = es.getDriver();
        Connection c = td.connect("jdbc:teiid:test", null);
        Statement s = c.createStatement();
        s.execute("select count(distinct name) from sys.tables where name like 'test%'");
        s.getResultSet().next();
        assertEquals(3, s.getResultSet().getInt(1));

        s.execute("select count(distinct name) from sys.columns where tablename like 'test%'");
        s.getResultSet().next();
        assertEquals(2, s.getResultSet().getInt(1));
    }

    @Test public void testCreateDomain() throws Exception {
        es.start(new EmbeddedConfiguration());

        es.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("domains-vdb.ddl")), true);

        Connection c = es.getDriver().connect("jdbc:teiid:domains", null);

        Statement s = c.createStatement();

        s.execute("select * from g1");

        ResultSetMetaData rsmd = s.getResultSet().getMetaData();
        //for now we'll report the runtime type
        assertEquals("string", rsmd.getColumnTypeName(1));
        ResultSet rs = c.getMetaData().getColumns(null, null, "g1", null);
        rs.next();
        assertEquals("x", rs.getString("TYPE_NAME"));
        rs.next();
        assertEquals("z", rs.getString("TYPE_NAME"));
        rs.next();
        assertEquals("x[]", rs.getString("TYPE_NAME"));

        try {
            s.execute("select cast(1 as a)"); //should fail
            fail();
        } catch (SQLException e) {

        }

        s.execute("select cast(1 as z)");
        rs = s.getResultSet();

        //for now we'll report the runtime type
        assertEquals("bigdecimal", rs.getMetaData().getColumnTypeName(1));

        s.execute("select xmlcast(xmlparse(document '<a>1</a>') as z)");

        s.execute("select attname, atttypid from pg_attribute where attname = 'e1'");
        rs = s.getResultSet();
        rs.next();
        assertEquals(1043, rs.getInt(2)); //varchar

        s.execute("select cast((1.0,) as z[])");
        rs = s.getResultSet();
        rs.next();
        assertArrayEquals(new BigDecimal[] {BigDecimal.valueOf(1.0)}, (BigDecimal[])rs.getArray(1).getArray());
        assertEquals("bigdecimal[]", rs.getMetaData().getColumnTypeName(1));
    }

    @Ignore("limit to/exclude not yet implemented")
    @Test public void testImportExcept() throws Exception {
        es.start(new EmbeddedConfiguration());
        es.addMetadataRepository("x", new MetadataRepository() {
            @Override
            public void loadMetadata(MetadataFactory factory,
                    ExecutionFactory executionFactory,
                    Object connectionFactory, String text)
                    throws TranslatorException {
                assertEquals("helloworld1,other", factory.getModelProperties().get("importer.excludeTables"));
                Table t = factory.addTable("helloworld");
                t.setVirtual(true);
                factory.addColumn("col", "string", t);
                t.setSelectTransformation("select 'HELLO WORLD'");
            }
        });
        String externalDDL = "CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "IMPORT FOREIGN SCHEMA public except (helloworld1, other) FROM REPOSITORY x INTO test2;";

        es.deployVDB(new ByteArrayInputStream(externalDDL.getBytes(Charset.forName("UTF-8"))), true);
        ResultSet rs = es.getDriver().connect("jdbc:teiid:test", null).createStatement().executeQuery("select * from helloworld");
        rs.next();
        assertEquals("HELLO WORLD", rs.getString(1));
    }

    @Test public void testDDLNameFormat() throws Exception {
        es.start(new EmbeddedConfiguration());
        es.addMetadataRepository("x", new MetadataRepository() {
            @Override
            public void loadMetadata(MetadataFactory factory,
                    ExecutionFactory executionFactory,
                    Object connectionFactory, String text)
                    throws TranslatorException {
                Table t = factory.addTable("helloworld");
                t.setVirtual(true);
                factory.addColumn("col", "string", t);
                t.setSelectTransformation("select 'HELLO WORLD'");
            }
        });
        String externalDDL = "CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2 options ( importer.nameFormat 'prod_%s');"
                + "IMPORT FOREIGN SCHEMA public FROM REPOSITORY x INTO test2;";

        es.deployVDB(new ByteArrayInputStream(externalDDL.getBytes(Charset.forName("UTF-8"))), true);
        ResultSet rs = es.getDriver().connect("jdbc:teiid:test", null).createStatement().executeQuery("select * from prod_helloworld");
        rs.next();
        assertEquals("HELLO WORLD", rs.getString(1));
    }

    @Test public void testFailOver() throws Exception {
        es.start(new EmbeddedConfiguration());

        es.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("domains-vdb.ddl")), true);

        Connection c = es.getDriver().connect("jdbc:teiid:domains;autoFailOver=true", null);

        Statement s = c.createStatement();

        s.execute("select * from g1");

        es.undeployVDB("domains");
        es.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("domains-vdb.ddl")), true);

        s.execute("select * from g1");
    }

    @Test public void testLateralTupleSourceReuse() throws Exception {
        es.start(new EmbeddedConfiguration());
        int rows = 20;
        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("y");
        mmd.addSourceMetadata("ddl", "CREATE VIRTUAL PROCEDURE pr0(arg1 string) returns (res1 string) AS\n" +
                "    BEGIN\n" +
                "        SELECT '2017-01-01';\n" +
                "    END;"
                + "create foreign table test_t1(col_t1 varchar) options (cardinality 20); create foreign table test_t2(col_t2 integer) options (cardinality 20);");
        mmd.addSourceMapping("y", "y", null);

        HardCodedExecutionFactory hcef = new HardCodedExecutionFactory();
        es.addTranslator("y", hcef);

        es.deployVDB("x", mmd);

        String sql = "SELECT d.col_t2 FROM \"test_t1\", table(CALL pr0(\"arg1\" => col_t1)) x\n" +
                "     join table(select * from \"test_t2\") d \n" +
                " on true " +
                "UNION all\n" +
                "SELECT d.col_t2 FROM \"test_t1\", table(CALL pr0(\"arg1\" => col_t1)) x\n" +
                "    join table(select * from \"test_t2\") d \n" +
                " on true " +
                "        limit 100";

        List<?>[] vals = new List<?>[rows];
        Arrays.fill(vals, Arrays.asList("1"));
        List<?>[] vals1 = new List<?>[rows];
        Arrays.fill(vals1, Arrays.asList(1));
        hcef.addData("SELECT test_t1.col_t1 FROM test_t1", Arrays.asList(vals));
        hcef.addData("SELECT test_t2.col_t2 FROM test_t2", Arrays.asList(vals1));

        Connection c = es.getDriver().connect("jdbc:teiid:x;", null);
        Statement s = c.createStatement();

        s.executeQuery(sql);

        ResultSet rs = s.getResultSet();
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        assertEquals(100, count);
    }

    @Test public void testOpenTracing() throws Exception {
        MockTracer tracer = new MockTracer();
        GlobalTracerInjector.setTracer(tracer);
        Logger logger = Mockito.mock(Logger.class);
        Mockito.stub(logger.isEnabled(LogConstants.CTX_COMMANDLOGGING, MessageLevel.DETAIL)).toReturn(false);
        Mockito.stub(logger.isEnabled(LogConstants.CTX_COMMANDLOGGING_SOURCE, MessageLevel.DETAIL)).toReturn(false);
        Logger old = org.teiid.logging.LogManager.setLogListener(logger);
        try {
            SocketConfiguration s = new SocketConfiguration();
            InetSocketAddress addr = new InetSocketAddress(0);
            s.setBindAddress(addr.getHostName());
            s.setPortNumber(addr.getPort());
            s.setProtocol(WireProtocol.teiid);
            EmbeddedConfiguration config = new EmbeddedConfiguration();
            config.addTransport(s);
            es.start(config);

            HardCodedExecutionFactory hcef = new HardCodedExecutionFactory();
            hcef.addData("SELECT t1.col_t1 FROM t1", Arrays.asList(Arrays.asList("a")));
            hcef.addData("SELECT t2.col_t2 FROM t2", Arrays.asList(Arrays.asList("b")));
            es.addTranslator("y", hcef);

            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("y");
            mmd.addSourceMetadata("ddl", "create foreign table t1(col_t1 varchar) options (cardinality 20); "
                    + "create foreign table t2(col_t2 varchar) options (cardinality 20);");
            mmd.addSourceMapping("y", "y", null);
            es.deployVDB("x", mmd);
            Connection c = es.getDriver().connect("jdbc:teiid:x;", null);
            Statement stmt = c.createStatement();

            ResultSet rs = stmt.executeQuery("select * from t1 union all select * from t2");
            while (rs.next()) {

            }
            stmt.close();

            List<MockSpan> spans = tracer.finishedSpans();
            assertEquals(0, spans.size());

            try (Scope ignored = tracer.buildSpan("some operation").startActive(true)) {
                assertNotNull(tracer.activeSpan());
                stmt = c.createStatement();
                //execute with an active span
                rs = stmt.executeQuery("select * from t1 union all select * from t2");
                while (rs.next()) {

                }
                stmt.close();
            }

            spans = tracer.finishedSpans();

            //parent span started here, and a child span for the query execution, 2 source queries
            assertEquals(spans.toString(), 4, spans.size());

            tracer.reset();

            //remote propagation
            Connection remote = TeiidDriver.getInstance().connect("jdbc:teiid:x@mm://"+addr.getHostName()+":"+es.transports.get(0).getPort(), null);

            try (Scope ignored = tracer.buildSpan("some remote operation").startActive(true)) {
                assertNotNull(tracer.activeSpan());
                stmt = remote.createStatement();
                //execute with an active span
                rs = stmt.executeQuery("select * from t1 union all select * from t2");
                while (rs.next()) {

                }
                stmt.close();
            }

            //this isn't ideal, but close is an async event
            for (int i = 0; i < 1000; i++) {
                spans = tracer.finishedSpans();
                if (spans.size() == 2) {
                    break;
                }
                Thread.sleep(10);
            }

            //parent span started here, and a child span for the query execution, 2 source queries
            assertEquals(4, spans.size());
        } finally {
            GlobalTracerInjector.setTracer(GlobalTracer.get());
            org.teiid.logging.LogManager.setLogListener(old);
        }
    }

    @Test public void testImportFunctions() throws Exception {
        es.start(new EmbeddedConfiguration());

        String ddl = "CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "IMPORT FOREIGN SCHEMA \"org.teiid.runtime.Funcs\" FROM REPOSITORY UDF INTO test2;"
                + "IMPORT FOREIGN SCHEMA \"java.lang.System\" FROM REPOSITORY UDF INTO test2;";

        es.deployVDB(new ByteArrayInputStream(ddl.getBytes("UTF-8")), true);
        ResultSet rs = es.getDriver().connect("jdbc:teiid:test", null).createStatement().executeQuery("select something(1), nanoTime()");
        rs.next();
        assertTrue(rs.getObject(1) instanceof Boolean);
        assertTrue(rs.getObject(2) instanceof Long);
    }

    @Test public void testLongRanksDefault() throws Exception {
        es.start(new EmbeddedConfiguration());

        String ddl = "CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "SET SCHEMA test2;"
                + "CREATE VIRTUAL VIEW x as select row_number() over (order by 1) rn from (select 1) as x";

        es.deployVDB(new ByteArrayInputStream(ddl.getBytes("UTF-8")), true);
        ResultSet rs = es.getDriver().connect("jdbc:teiid:test", null).createStatement().executeQuery("select * from x");
        rs.next();
        assertTrue(rs.getObject(1) instanceof Long);
    }

    @Test public void testTemporaryLobs() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);

        String ddl = "CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "SET SCHEMA test2;"
                + "CREATE VIRTUAL VIEW x as select 1 as col";

        es.deployVDB(new ByteArrayInputStream(ddl.getBytes("UTF-8")), true);

        Connection connection = es.getDriver().connect("jdbc:teiid:test", null);
        Statement stmt = connection.createStatement();
        stmt.execute("set autoCommitTxn off");

        PreparedStatement ps = connection.prepareStatement("insert into #temp select 1 as x, cast(? as clob) y");
        ps.setClob(1, new StringReader(new String(new char[4000])));
        ps.execute();

        //ensure that a temporary memory lob is still usable
        stmt.execute("insert into #temp select 2, concat((select y from #temp where x = 1), (select y from #temp where x = 1))");

        //keep making it larger to trigger disk backing, and make sure it's still usable
        stmt.execute("insert into #temp select 3, concat((select y from #temp where x = 2), (select y from #temp where x = 2))");
        stmt.execute("insert into #temp select 4, concat((select y from #temp where x = 3), (select y from #temp where x = 3))");
        stmt.execute("insert into #temp select 5, concat((select y from #temp where x = 4), (select y from #temp where x = 4))");
        stmt.execute("insert into #temp select 6, concat((select y from #temp where x = 5), (select y from #temp where x = 5))");
    }

    @Test public void testSessionKilling() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setMemoryBufferSpace(1);
        ec.setMaxReserveKb(1);
        ec.setMaxBufferSpace(1);
        ec.setMaxActivePlans(2);
        ec.setMaxStorageObjectSize(6000000);
        es.start(ec);

        String ddl = "CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "SET SCHEMA test2;"
                + "CREATE VIRTUAL VIEW x as select 1 as col";

        es.deployVDB(new ByteArrayInputStream(ddl.getBytes("UTF-8")), true);
        Connection connection = es.getDriver().connect("jdbc:teiid:test", null);
        Statement stmt = connection.createStatement();
        stmt.execute("set autoCommitTxn off");
        PreparedStatement ps = connection.prepareStatement("insert into #temp select 1 as x, cast(? as clob) y");
        ps.setClob(1, new StringReader(new String(new char[65000])));
        ps.execute();
        boolean killed = false;
        for (int i = 0; i < 10; i++) {
            try {
                stmt.execute("insert into #temp select 2 as x, concat((select y from #temp where x = 1), (select y from #temp where x = 1))");
            } catch (SQLException e) {
                assertTrue(i > 5);
                //session killed
                killed = true;
                break;
            }
        }
        assertTrue(killed);

        //same setup, but don't push over the limit yet
        connection = es.getDriver().connect("jdbc:teiid:test", null);
        stmt = connection.createStatement();
        stmt.execute("set autoCommitTxn off");
        ps = connection.prepareStatement("insert into #temp select 1 as x, cast(? as clob) y");
        ps.setClob(1, new StringReader(new String(new char[65000])));
        ps.execute();
        for (int i = 0; i < 6; i++) {
            stmt.execute("insert into #temp select 2 as x, concat((select y from #temp where x = 1), (select y from #temp where x = 1))");
        }

        Connection connection2 = es.getDriver().connect("jdbc:teiid:test", null);
        Statement stmt2 = connection2.createStatement();
        stmt2.execute("set autoCommitTxn off");
        PreparedStatement ps2 = connection2.prepareStatement("insert into #temp select 1 as x, cast(? as clob) y");
        ps2.setClob(1, new StringReader(new String(new char[65000])));
        ps2.execute();
        for (int i = 0; i < 3; i++) {
            try {
                stmt2.execute("insert into #temp select 2 as x, concat((select y from #temp where x = 1), (select y from #temp where x = 1))");
            } catch (SQLException e) {
                //ideally this would always succeed, but that can't yet be assured
                break;
            }
        }

        try {
            stmt.executeQuery("select * from #temp");
            fail();
        } catch (SQLException e) {
            //should have been killed - it's the largest
        }

      //ensure the other is still valid - we can't yet make this guarentee
      //stmt2.executeQuery("select * from #temp");
    }

    @Test public void testSessionKillingWithTables() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        ec.setMemoryBufferSpace(1);
        ec.setMaxReserveKb(1);
        ec.setMaxBufferSpace(1);
        ec.setMaxActivePlans(2);
        ec.setMaxStorageObjectSize(6000000);
        ec.setMaxProcessingKb(1);
        es.start(ec);

        String ddl = "CREATE DATABASE test VERSION '1';"
                + "USE DATABASE test VERSION '1';"
                + "CREATE VIRTUAL SCHEMA test2;"
                + "SET SCHEMA test2;"
                + "CREATE VIRTUAL VIEW x as select 1 as col";

        es.deployVDB(new ByteArrayInputStream(ddl.getBytes("UTF-8")), true);
        Connection connection = es.getDriver().connect("jdbc:teiid:test", null);
        Statement stmt = connection.createStatement();
        stmt.execute("set autoCommitTxn off");
        boolean killed = false;
        for (int i = 0; i < 64; i++) {
            try {
                stmt.execute("insert into #temp select * from sys.columns limit 400");
            } catch (SQLException e) {
                assertTrue(i > 50);
                //session killed
                killed = true;
                break;
            }
        }
        assertTrue(killed);

        //same setup, but don't push over the limit yet
        connection = es.getDriver().connect("jdbc:teiid:test", null);
        stmt = connection.createStatement();
        stmt.execute("set autoCommitTxn off");
        for (int i = 0; i < 50; i++) {
            stmt.execute("insert into #temp select * from sys.columns limit 400");
        }

        Connection connection2 = es.getDriver().connect("jdbc:teiid:test", null);
        Statement stmt2 = connection2.createStatement();
        stmt2.execute("set autoCommitTxn off");
        for (int i = 0; i < 10; i++) {
            try {
                stmt2.execute("insert into #temp select * from sys.columns limit 400");
            } catch (SQLException e) {
                //ideally this would always succeed, but that can't yet be assured
                break;
            }
        }

        try {
            stmt.executeQuery("select * from #temp");
            fail();
        } catch (SQLException e) {
            //should have been killed - it's the largest
        }

        //ensure the other is still valid - we can't yet make this guarentee
        //stmt2.executeQuery("select * from #temp");
    }

    @Test public void testJsonPath() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);

        es.deployVDB(new ByteArrayInputStream(createVDB("x", "1.0.0").getBytes("UTF-8")));

        Connection connection = es.getDriver().connect("jdbc:teiid:x", null);
        Statement stmt = connection.createStatement();

        String sql = "select jsonpathvalue(jsonparse('{\"a\":1, \"b\":[2,3]}', false), '$.b')";

        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        assertEquals("[2,3]", rs.getString(1));
    }

    /**
     * should fail validation as the hidden view still requires qualification
     * @throws Exception
     */
    @Test(expected = VirtualDatabaseException.class) public void testHiddenAlwaysQualified() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        es.deployVDB(new ByteArrayInputStream(("<vdb name=\"hidden\" version=\"1\">"
                + "<property name=\"hidden-qualified\" value=\"true\"/>"
                + "<model visible=\"false\" type=\"VIRTUAL\" name=\"x\">"
                + "         <metadata type=\"DDL\"><![CDATA["
                + "        CREATE VIEW TestView (c1 integer) AS SELECT 1;"
                + "       ]]>"
                + "</metadata></model>"
                + "<model visible=\"true\" type=\"VIRTUAL\" name=\"y\">"
                + "         <metadata type=\"DDL\"><![CDATA["
                + "        CREATE VIEW TestView2 (c1 integer) AS select * from testview;"
                + "       ]]>"
                + "</metadata></model>"
                + "</vdb>").getBytes()));
    }

    @Test public void testLargeCopyLobs() throws Exception {
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
        es.start(ec);
        HardCodedExecutionFactory hcef = new HardCodedExecutionFactory();
        hcef.addData("SELECT t.col FROM t", Arrays.asList(Arrays.asList(new ClobType(new ClobImpl(new InputStreamFactory() {

            @Override
            public InputStream getInputStream() throws IOException {
                return new ByteArrayInputStream(new byte[DataTypeManager.MAX_LOB_MEMORY_BYTES*2]);
            }

        }, DataTypeManager.MAX_LOB_MEMORY_BYTES)))));
        hcef.setCopyLobs(true);

        es.addTranslator("y", hcef);

        ModelMetaData mmd = new ModelMetaData();
        mmd.setName("my-schema");
        mmd.addSourceMapping("x", "y", null);
        mmd.addSourceMetadata("ddl", "create foreign table t (col clob);");

        es.deployVDB("test", mmd);

        TeiidDriver td = es.getDriver();
        Connection c = td.connect("jdbc:teiid:test", null);
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("select cast(t.col as string) from t");
        assertTrue(rs.next());
        assertEquals(DataTypeManager.MAX_STRING_LENGTH, rs.getString(1).length());
    }

}
