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

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.SimpleMock;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.VDBRepository;
import org.teiid.metadata.Column;
import org.teiid.metadata.DuplicateRecordException;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.NativeMetadataRepository;
import org.teiid.query.parser.QueryParser;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.oracle.OracleExecutionFactory;
import org.teiid.translator.jdbc.teiid.TeiidExecutionFactory;

/**
 */
@SuppressWarnings("nls")
public class TestDynamicImportedMetaData {

    public static final class BadMetadata {
        public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
            throw new SQLException();
        }
    }

    private FakeServer server;

    @Before public void setup() {
        this.server = new FakeServer(true);
    }

    @After public void tearDown() {
        this.server.stop();
    }

    private MetadataFactory getMetadata(Properties importProperties, Connection conn)
            throws TranslatorException {
        MetadataFactory mf = createMetadataFactory("test", importProperties);

        TeiidExecutionFactory tef = new TeiidExecutionFactory();
        tef.getMetadata(mf, conn);
        return mf;
    }

    private MetadataFactory createMetadataFactory(String schema, Properties importProperties) {
        VDBRepository vdbRepository = new VDBRepository();
        return new MetadataFactory("vdb", 1, schema, vdbRepository.getRuntimeTypeMap(), importProperties, null);
    }

    @Test public void testUniqueReferencedKey() throws Exception {
        server.deployVDB("vdb", UnitTestUtil.getTestDataPath() + "/keys.vdb");
        Connection conn = server.createConnection("jdbc:teiid:vdb"); //$NON-NLS-1$

        Properties importProperties = new Properties();
        importProperties.setProperty("importer.importKeys", "true");
        importProperties.setProperty("importer.schemaPattern", "x");
        importProperties.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());
        MetadataFactory mf = getMetadata(importProperties, conn);
        Table t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("VDB.X.A");
        List<ForeignKey> fks = t.getForeignKeys();
        assertEquals(1, fks.size());
        assertNotNull(fks.get(0).getPrimaryKey());
    }

    @Test public void testProcImport() throws Exception {
        server.deployVDB("vdb", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
        Connection conn = server.createConnection("jdbc:teiid:vdb"); //$NON-NLS-1$

        Properties importProperties = new Properties();
        importProperties.setProperty("importer.importProcedures", Boolean.TRUE.toString());
        importProperties.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());
        MetadataFactory mf = getMetadata(importProperties, conn);
        Procedure p = mf.asMetadataStore().getSchemas().get("TEST").getProcedures().get("VDB.SYS.ARRAYITERATE");
        assertEquals(1, p.getResultSet().getColumns().size());
    }

    @Test public void testExcludes() throws Exception {
        server.deployVDB("vdb", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
        Connection conn = server.createConnection("jdbc:teiid:vdb"); //$NON-NLS-1$

        Properties importProperties = new Properties();
        importProperties.setProperty("importer.importProcedures", Boolean.TRUE.toString());
        importProperties.setProperty("importer.excludeTables", "VDB\\.(SYS|pg_catalog).*");
        importProperties.setProperty("importer.excludeProcedures", "VDB\\..*");
        MetadataFactory mf = getMetadata(importProperties, conn);
        assertEquals(String.valueOf(mf.asMetadataStore().getSchemas().get("TEST").getTables()), 3, mf.asMetadataStore().getSchemas().get("TEST").getTables().size());
        assertEquals(0, mf.asMetadataStore().getSchemas().get("TEST").getProcedures().size());
    }

    @Test public void testDuplicateException() throws Exception {
        MetadataFactory mf = createMetadataFactory("x", new Properties());
        MetadataFactory mf1 = createMetadataFactory("y", new Properties());

        Table dup = mf.addTable("dup");
        Table dup1 = mf1.addTable("dup");

        mf.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup);
        mf1.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup1);

        MetadataStore ms = mf.asMetadataStore();
        ms.addSchema(mf1.asMetadataStore().getSchemas().values().iterator().next());

        server.deployVDB("test", ms);
        Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$

        Properties importProperties = new Properties();

        importProperties.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());

        mf = getMetadata(importProperties, conn);
        Table t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("TEST.X.DUP");
        assertEquals("\"test\".\"x\".\"dup\"", t.getNameInSource());

        importProperties.setProperty("importer.useFullSchemaName", Boolean.FALSE.toString());
        try {
            getMetadata(importProperties, conn);
            fail();
        } catch (DuplicateRecordException e) {

        }
    }

    @Test public void testUseCatalog() throws Exception {
        MetadataFactory mf = createMetadataFactory("x", new Properties());

        Table dup = mf.addTable("dup");

        mf.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup);

        MetadataStore ms = mf.asMetadataStore();

        server.deployVDB("test", ms);
        Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$

        Properties importProperties = new Properties();
        importProperties.setProperty("importer.useCatalogName", Boolean.FALSE.toString());
        importProperties.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());
        mf = getMetadata(importProperties, conn);
        Table t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("X.DUP");
        assertEquals("\"x\".\"dup\"", t.getNameInSource());
    }

    public static class DatabaseMetaDataProxy {
        public String getCatalogSeparator() {
            return ":";
        }
    }

    public static class ConnectionProxy {

        private final Connection conn;

        private ConnectionProxy(Connection conn) {
            this.conn = conn;
        }

        public DatabaseMetaData getMetaData() throws SQLException {
            DatabaseMetaData dmd = conn.getMetaData();
            dmd = (DatabaseMetaData) SimpleMock.createSimpleMock(new Object[] {new DatabaseMetaDataProxy(), dmd}, new Class<?>[] {DatabaseMetaData.class});
            return dmd;
        }
    }

    @Test public void testUseCatalogSeparator() throws Exception {
        MetadataFactory mf = createMetadataFactory("x", new Properties());

        Table dup = mf.addTable("dup");

        mf.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup);

        MetadataStore ms = mf.asMetadataStore();

        server.deployVDB("test", ms);
        final Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$

        Properties importProperties = new Properties();
        importProperties.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());
        Connection conn1 = (Connection) SimpleMock.createSimpleMock(new Object[] {new ConnectionProxy(conn), conn}, new Class<?>[] {Connection.class});
        mf = getMetadata(importProperties, conn1);
        Table t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("test:X.DUP");
        assertEquals("\"test\":\"x\".\"dup\"", t.getNameInSource());
    }

    @Test public void testUseQualified() throws Exception {
        MetadataFactory mf = createMetadataFactory("x", new Properties());

        Table dup = mf.addTable("dup");

        mf.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup);

        MetadataStore ms = mf.asMetadataStore();

        server.deployVDB("test", ms);
        Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$

        //neither the name nor name in source should be qualified
        Properties importProperties = new Properties();
        importProperties.setProperty("importer.useQualifiedName", Boolean.FALSE.toString());
        importProperties.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());

        mf = getMetadata(importProperties, conn);
        Table t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("DUP");
        assertEquals("\"dup\"", t.getNameInSource());
    }

    @Test
    public void testDDLMetadata() throws Exception {
        String ddl = "CREATE FOREIGN PROCEDURE getTextFiles(IN pathAndPattern varchar) RETURNS (file clob, filpath string) OPTIONS(UUID 'uuid')";
        MetadataFactory mf = createMetadataFactory("MarketData", new Properties());
        QueryParser.getQueryParser().parseDDL(mf, ddl);
        MetadataStore ms = mf.asMetadataStore();

        String ddl2 = "CREATE VIEW stock (symbol string, price bigdecimal) OPTIONS (UUID 'uuid')" +
                "AS select stock.* from (call MarketData.getTextFiles('*.txt')) f, " +
                "TEXTTABLE(f.file COLUMNS symbol string, price bigdecimal HEADER) stock;";
        MetadataFactory m2 = createMetadataFactory("portfolio", new Properties());

        QueryParser.getQueryParser().parseDDL(m2, ddl2);
        m2.getSchema().setPhysical(false);
        m2.mergeInto(ms);

        server.deployVDB("test", ms);
        Connection conn =  server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$
        Properties props = new Properties();
        props.setProperty("importer.importProcedures", Boolean.TRUE.toString());
        props.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());
        MetadataStore store = getMetadata(props, conn).asMetadataStore();

        Procedure p = store.getSchema("test").getProcedure("test.MarketData.getTextFiles");
        assertNotNull(p);

        ProcedureParameter pp = p.getParameters().get(0);
        assertEquals("pathAndPattern", pp.getName());
        assertEquals("\"pathAndPattern\"", pp.getNameInSource());
        assertEquals(ProcedureParameter.Type.In, pp.getType());
        //assertEquals("string", pp.getDatatype().getName());

        Table t = store.getSchema("test").getTable("test.portfolio.stock");
        assertNotNull(t);

        List<Column> columns = t.getColumns();
        assertEquals(2, columns.size());
        assertEquals("symbol", columns.get(0).getName());
        assertEquals("price", columns.get(1).getName());
    }

    @Test public void testImportFunction() throws Exception {
        MetadataFactory mf = createMetadataFactory("x", new Properties());

        Table dup = mf.addTable("dup");

        mf.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup);

        MetadataStore ms = mf.asMetadataStore();

        server.deployVDB("test", ms);
        Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$

        Properties importProperties = new Properties();
        importProperties.setProperty(NativeMetadataRepository.IMPORT_PUSHDOWN_FUNCTIONS, Boolean.TRUE.toString());
        importProperties.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());

        mf = createMetadataFactory("test", importProperties);
        NativeMetadataRepository nmr = new NativeMetadataRepository();
        OracleExecutionFactory oef = new OracleExecutionFactory();
        oef.start();
        DataSource ds = Mockito.mock(DataSource.class);

        Mockito.stub(ds.getConnection()).toReturn(conn);

        nmr.loadMetadata(mf, oef, ds);

        Map<String, FunctionMethod> functions = mf.asMetadataStore().getSchemas().get("TEST").getFunctions();

        assertEquals(18, functions.size());
    }

    @Test public void testIgnorePkIndex() throws Exception {
        MetadataFactory mf = createMetadataFactory("x", new Properties());

        Table dup = mf.addTable("x");

        mf.addColumn("x", DataTypeManager.DefaultDataTypes.STRING, dup);
        mf.addPrimaryKey("foo", Arrays.asList("x"), dup);

        MetadataStore ms = mf.asMetadataStore();

        server.deployVDB("test", ms);

        //cheat and add the index after deployment
        mf.addIndex("foo", false, Arrays.asList("x"), dup);

        Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$

        Properties importProperties = new Properties();
        importProperties.setProperty("importer.importKeys", Boolean.TRUE.toString());
        importProperties.setProperty("importer.importIndexes", Boolean.TRUE.toString());
        importProperties.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());
        mf = getMetadata(importProperties, conn);
        Table t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("test.X.X");
        assertNotNull(t.getPrimaryKey());
        assertEquals(0, t.getUniqueKeys().size());
        assertEquals(0, t.getIndexes().size());
    }

    @Test public void testDroppedFk() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.addSourceMetadata("ddl", "create foreign table x (y integer primary key);"
                + "create foreign table z (y integer, foreign key (y) references x)");
        mmd.setName("foo");
        mmd.addSourceMapping("x", "x", "x");
        server.addTranslator("x", new ExecutionFactory());
        server.deployVDB("vdb", mmd);
        Connection conn = server.createConnection("jdbc:teiid:vdb"); //$NON-NLS-1$

        Properties importProperties = new Properties();
        importProperties.setProperty("importer.importKeys", "true");
        importProperties.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());
        //only import z and not the referenced table x
        importProperties.setProperty("importer.tableNamePattern", "z");
        MetadataFactory mf = getMetadata(importProperties, conn);
        Table t = mf.asMetadataStore().getSchemas().get("test").getTables().get("vdb.foo.z");
        List<ForeignKey> fks = t.getForeignKeys();
        assertEquals(0, fks.size());
    }

    @Test public void testMultipleFK() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.addSourceMetadata("ddl", "create foreign table x (y integer, z integer, primary key (y, z));"
                + "create foreign table z (y integer, z integer, y1 integer, z1 integer, foreign key (y, z) references x (y, z), foreign key (y1, z1) references x (y, z))");
        mmd.setName("foo");
        mmd.addSourceMapping("x", "x", "x");
        server.addTranslator("x", new ExecutionFactory());
        server.deployVDB("vdb", mmd);
        Connection conn = server.createConnection("jdbc:teiid:vdb"); //$NON-NLS-1$

        Properties importProperties = new Properties();
        importProperties.setProperty("importer.importKeys", "true");
        importProperties.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());
        MetadataFactory mf = getMetadata(importProperties, conn);
        Table t = mf.asMetadataStore().getSchemas().get("test").getTables().get("vdb.foo.z");
        List<ForeignKey> fks = t.getForeignKeys();
        assertEquals(2, fks.size());
    }

    @Test public void testMultiSource() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.addSourceMetadata("ddl", "create foreign table x (y integer primary key);");
        mmd.setName("foo");
        mmd.addSourceMapping("x", "x", "x");
        mmd.addProperty("importer.useFullSchemaName", "true");
        server.addTranslator("x", new ExecutionFactory());
        server.deployVDB("vdb", mmd);
        TeiidExecutionFactory tef = new TeiidExecutionFactory() {
            @Override
            public void closeConnection(Connection connection,
                    DataSource factory) {
            }
        };
        tef.setSupportsDirectQueryProcedure(true);
        tef.start();
        server.addTranslator("teiid", tef);
        DataSource ds = Mockito.mock(DataSource.class);
        Mockito.stub(ds.getConnection()).toReturn(server.getDriver().connect("jdbc:teiid:vdb", null));
        server.addConnectionFactory("teiid1", ds);
        server.addConnectionFactory("teiid2", ds);
        server.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("multi.xml")));
        Connection c = server.createConnection("jdbc:teiid:multi", null);
        Statement s = c.createStatement();

        s.execute("call native('select ?', 'b')");

        ResultSet rs = s.getResultSet();
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());

        s.execute("call native(request=>'select ?', variable=>('b',), target=>'teiid1')");
        rs = s.getResultSet();

        assertTrue(rs.next());

        Object[] result = (Object[]) rs.getArray(1).getArray();
        assertArrayEquals(new Object[] {"b"}, result);
        assertFalse(rs.next());
    }

    @Test public void testMultiSourceImportError() throws Exception {
        ModelMetaData mmd = new ModelMetaData();
        mmd.addSourceMetadata("ddl", "create foreign table x (y integer primary key);");
        mmd.setName("foo");
        mmd.addSourceMapping("x", "x", "x");
        mmd.addProperty("importer.useFullSchemaName", "true");
        server.addTranslator("x", new ExecutionFactory());
        server.deployVDB("vdb", mmd);
        TeiidExecutionFactory tef = new TeiidExecutionFactory() {
            @Override
            public void closeConnection(Connection connection,
                    DataSource factory) {
            }
        };
        tef.start();
        server.addTranslator("teiid", tef);
        DataSource ds = Mockito.mock(DataSource.class);
        Connection c = server.getDriver().connect("jdbc:teiid:vdb", null);
        //bad databasemetadata
        DatabaseMetaData dbmd = (DatabaseMetaData) SimpleMock.createSimpleMock(new Object[] {new BadMetadata(), c.getMetaData()}, new Class[] {DatabaseMetaData.class});
        Connection mock = Mockito.mock(Connection.class);
        Mockito.stub(mock.getMetaData()).toReturn(dbmd);
        Mockito.stub(ds.getConnection()).toReturn(mock);

        DataSource ds1 = Mockito.mock(DataSource.class);
        Mockito.stub(ds1.getConnection()).toReturn(server.getDriver().connect("jdbc:teiid:vdb", null));

        server.addConnectionFactory("teiid1", ds);
        server.addConnectionFactory("teiid2", ds1);
        server.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("multi.xml")));
    }

    @Test public void testUseScale() throws Exception {
        MetadataFactory mf = createMetadataFactory("x", new Properties());

        Table dup = mf.addTable("x");

        Column c = mf.addColumn("x", DataTypeManager.DefaultDataTypes.BIG_DECIMAL, dup);
        c.setPrecision(10);
        c.setScale(2);

        MetadataStore ms = mf.asMetadataStore();

        server.deployVDB("test", ms);
        Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$

        Properties importProperties = new Properties();
        importProperties.setProperty("importer.useQualifiedName", Boolean.FALSE.toString());
        importProperties.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());

        mf = getMetadata(importProperties, conn);
        Table t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("x");
        c = t.getColumnByName("x");
        assertEquals(10, c.getPrecision());
        assertEquals(2, c.getScale());
    }

    public final class TypeMixin {
        public ResultSet getTypeInfo() throws SQLException {
            try {
                Connection conn = server.createConnection("jdbc:teiid:test");
                return conn.createStatement().executeQuery("select 'long', " +Types.BIGINT + ", null, null, null, null, null, null, null, true");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test public void testUBigInt() throws Exception {
        MetadataFactory mf = createMetadataFactory("x", new Properties());

        Table x = mf.addTable("x");

        mf.addColumn("x", DataTypeManager.DefaultDataTypes.LONG, x);

        MetadataStore ms = mf.asMetadataStore();

        server.deployVDB("test", ms);
        Connection conn = server.createConnection("jdbc:teiid:test"); //$NON-NLS-1$

        Properties importProperties = new Properties();
        importProperties.setProperty("importer.useQualifiedName", Boolean.FALSE.toString());
        importProperties.setProperty("importer.useFullSchemaName", Boolean.TRUE.toString());

        DatabaseMetaData dbmd = (DatabaseMetaData) SimpleMock.createSimpleMock(new Object[] {new TypeMixin(), conn.getMetaData()}, new Class[] {DatabaseMetaData.class});
        Connection c = Mockito.mock(Connection.class);
        Mockito.stub(c.getMetaData()).toReturn(dbmd);

        mf = getMetadata(importProperties, c);
        Table t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("X");
        assertEquals("bigdecimal", t.getColumnByName("x").getRuntimeType());
        assertEquals(19, t.getColumnByName("x").getPrecision());

        importProperties.setProperty("importer.useIntegralTypes", Boolean.TRUE.toString());

        mf = getMetadata(importProperties, c);
        t = mf.asMetadataStore().getSchemas().get("TEST").getTables().get("X");
        assertEquals("biginteger", t.getColumnByName("x").getRuntimeType());
    }
}
