package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.impl.VDBImportMetadata;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer.DeployVDBParameter;
import org.teiid.runtime.HardCodedExecutionFactory;


@SuppressWarnings("nls")
public class TestVDBMerge extends AbstractMMQueryTestCase {

    private static final String VDB1 = "PartsSupplier"; //$NON-NLS-1$
    private static final String VDB2 = "QT_Ora9DS"; //$NON-NLS-1$

    private FakeServer server;

    @Before public void setup() throws Exception {
        server = new FakeServer(true);
        server.setThrowMetadataErrors(false); //test vdb has errors
    }

    @After public void teardown() throws Exception {
        server.stop();
    }

    @Test
    public void testMerge() throws Throwable {

        server.deployVDB(VDB1, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
        this.internalConnection = server.createConnection("jdbc:teiid:"+VDB1);

       execute("select * from sys.tables where schemaname ='PartsSupplier'"); //$NON-NLS-1$
       TestMMDatabaseMetaData.compareResultSet("TestVDBMerge/merge.test", this.internalResultSet);

       execute("select * from sys.tables where schemaname='BQT1'"); //$NON-NLS-1$
       TestMMDatabaseMetaData.compareResultSet("TestVDBMerge/merge.before", this.internalResultSet);

       this.internalConnection.close();

       server.deployVDB(VDB2, UnitTestUtil.getTestDataPath()+"/QT_Ora9DS_1.vdb");

       DeployVDBParameter param = new DeployVDBParameter(null, null);
       VDBImportMetadata vdbImport = new VDBImportMetadata();
       vdbImport.setName(VDB2);
       vdbImport.setVersion("1");
       param.vdbImports = Arrays.asList(vdbImport);
       server.removeVDB(VDB1);
       server.deployVDB(VDB1, UnitTestUtil.getTestDataPath()+"/PartsSupplier.vdb", param);

       this.internalConnection = server.createConnection("jdbc:teiid:"+VDB1);
       execute("select * from sys.tables where schemaname='BQT1' order by name"); //$NON-NLS-1$
       TestMMDatabaseMetaData.compareResultSet("TestVDBMerge/merge.after", this.internalResultSet);
    }

    @Test
    public void testMergeWithEmptyVDB() throws Exception {
        server.deployVDB("empty", UnitTestUtil.getTestDataPath() + "/empty.vdb");
        this.internalConnection = server.createConnection("jdbc:teiid:empty");

        execute("select * from sys.tables where schemaname ='BQT1'"); //$NON-NLS-1$
        TestMMDatabaseMetaData.compareResultSet("TestVDBMerge/mergeEmpty.before", this.internalResultSet);
        this.internalConnection.close();

        server.deployVDB(VDB2, UnitTestUtil.getTestDataPath()+"/QT_Ora9DS_1.vdb");

        DeployVDBParameter param = new DeployVDBParameter(null, null);
        VDBImportMetadata vdbImport = new VDBImportMetadata();
        vdbImport.setName(VDB2);
        vdbImport.setVersion("1");
        param.vdbImports = Arrays.asList(vdbImport);
        server.undeployVDB("empty");
        server.deployVDB("empty", UnitTestUtil.getTestDataPath() + "/empty.vdb", param);

        this.internalConnection = server.createConnection("jdbc:teiid:empty");
        execute("select * from sys.tables where schemaname='BQT1'"); //$NON-NLS-1$
        TestMMDatabaseMetaData.compareResultSet("TestVDBMerge/mergeEmpty.after", this.internalResultSet);
    }

    @Test public void testMergeWithPolicies() throws Exception {
        server.deployVDB(new ByteArrayInputStream(new String("<vdb name=\"role-1\" version=\"1\">"
                + "<model name=\"myschema\" type=\"virtual\">"
                + "<metadata type = \"DDL\"><![CDATA[CREATE VIEW vw as select 'a' as col;]]></metadata></model>"
                + "<data-role name=\"y\" any-authenticated=\"true\"/></vdb>").getBytes()));
        this.internalConnection = server.createConnection("jdbc:teiid:role-1");

        try {
            execute("select * from vw"); //$NON-NLS-1$
            fail("should not be authorized");
        } catch (SQLException e) {

        }

        server.deployVDB(new ByteArrayInputStream(new String("<vdb name=\"role-2\" version=\"1\">"
                + "<import-vdb name=\"role-1\" version=\"1\"/>"
                + "</vdb>").getBytes()));
        this.internalConnection = server.createConnection("jdbc:teiid:role-2");

        try {
            execute("select * from vw"); //$NON-NLS-1$
            fail("should not be authorized");
        } catch (SQLException e) {

        }

        server.deployVDB(new ByteArrayInputStream(new String("<vdb name=\"role-3\" version=\"1\">"
                + "<import-vdb name=\"role-1\" version=\"1\" import-data-policies=\"false\"/>"
                + "</vdb>").getBytes()));
        this.internalConnection = server.createConnection("jdbc:teiid:role-3");

        execute("select * from vw"); //$NON-NLS-1$
    }

    @Test public void testMergeWithMultiSource() throws Exception {
        HardCodedExecutionFactory hc = new HardCodedExecutionFactory();
        hc.addData("SELECT tbl.col FROM tbl", Arrays.asList(Arrays.asList("a")));
        server.addTranslator("hc", hc);
        server.deployVDB(new ByteArrayInputStream(new String("<vdb name=\"ms-base\" version=\"1\">"
                + "<model name=\"myschema\"><source name=\"a\" translator-name=\"hc\"/><source name=\"b\" translator-name=\"hc\"/>"
                + "<metadata type = \"DDL\"><![CDATA[CREATE foreign table tbl (col string);]]></metadata></model>"
                + "</vdb>").getBytes()));
        this.internalConnection = server.createConnection("jdbc:teiid:ms-base");

        execute("select * from tbl"); //$NON-NLS-1$
        assertRowCount(2);

        server.deployVDB(new ByteArrayInputStream(new String("<vdb name=\"ms-2\" version=\"1\">"
                + "<import-vdb name=\"ms-base\" version=\"1\"/>"
                + "</vdb>").getBytes()));
        this.internalConnection = server.createConnection("jdbc:teiid:ms-2");

        execute("select * from tbl"); //$NON-NLS-1$
        assertRowCount(2);
    }
}
