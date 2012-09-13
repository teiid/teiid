package org.teiid.jdbc;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.impl.VDBImportMetadata;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer.DeployVDBParameter;


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
    	
       execute("select * from tables where schemaname ='PartsSupplier'"); //$NON-NLS-1$
       TestMMDatabaseMetaData.compareResultSet("TestVDBMerge/merge.test", this.internalResultSet);
       
       execute("select * from tables where schemaname='BQT1'"); //$NON-NLS-1$
       TestMMDatabaseMetaData.compareResultSet("TestVDBMerge/merge.before", this.internalResultSet);
       
       this.internalConnection.close();

       server.deployVDB(VDB2, UnitTestUtil.getTestDataPath()+"/QT_Ora9DS_1.vdb");

       DeployVDBParameter param = new DeployVDBParameter(null, null);
       VDBImportMetadata vdbImport = new VDBImportMetadata();
       vdbImport.setName(VDB2);
       param.vdbImports = Arrays.asList(vdbImport);
       server.removeVDB(VDB1);
       server.deployVDB(VDB1, UnitTestUtil.getTestDataPath()+"/PartsSupplier.vdb", param);
       
       this.internalConnection = server.createConnection("jdbc:teiid:"+VDB1);
       execute("select * from tables where schemaname='BQT1' order by name"); //$NON-NLS-1$
       TestMMDatabaseMetaData.compareResultSet("TestVDBMerge/merge.after", this.internalResultSet);
    }
	
    @Test
    public void testMergeWithEmptyVDB() throws Exception {
		server.deployVDB("empty", UnitTestUtil.getTestDataPath() + "/empty.vdb");
    	this.internalConnection = server.createConnection("jdbc:teiid:empty");
    
    	execute("select * from tables where schemaname ='BQT1'"); //$NON-NLS-1$
    	TestMMDatabaseMetaData.compareResultSet("TestVDBMerge/mergeEmpty.before", this.internalResultSet);
        this.internalConnection.close();
        
        server.deployVDB(VDB2, UnitTestUtil.getTestDataPath()+"/QT_Ora9DS_1.vdb");
        
        DeployVDBParameter param = new DeployVDBParameter(null, null);
        VDBImportMetadata vdbImport = new VDBImportMetadata();
        vdbImport.setName(VDB2);
        param.vdbImports = Arrays.asList(vdbImport);
        server.undeployVDB("empty");
        server.deployVDB("empty", UnitTestUtil.getTestDataPath() + "/empty.vdb", param);

        this.internalConnection = server.createConnection("jdbc:teiid:empty");
        execute("select * from tables where schemaname='BQT1'"); //$NON-NLS-1$
        TestMMDatabaseMetaData.compareResultSet("TestVDBMerge/mergeEmpty.after", this.internalResultSet);
    }
}
