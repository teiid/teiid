package org.teiid.jdbc;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;


@SuppressWarnings("nls")
public class TestVDBMerge extends AbstractMMQueryTestCase {
	
	private static final String VDB1 = "PartsSupplier"; //$NON-NLS-1$
	private static final String VDB2 = "QT_Ora9DS"; //$NON-NLS-1$
	FakeServer server = new FakeServer();
	
	@Test
    public void testMerge() throws Throwable {
		
		server.deployVDB(VDB1, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
    	this.internalConnection = server.createConnection("jdbc:teiid:"+VDB1);
    	
    	String[] expected = {
    			"VDBName[string]    SchemaName[string]    Name[string]    Type[string]    NameInSource[string]    IsPhysical[boolean]    SupportsUpdates[boolean]    UID[string]    Cardinality[integer]    Description[string]    IsSystem[boolean]    IsMaterialized[boolean]    OID[integer]",
    			"PartsSupplier    PartsSupplier    PARTSSUPPLIER.PARTS    Table    PARTS    true    true    mmuuid:f6276601-73fe-1edc-a81c-ecf397b10590    16    null    false    false    1",
    			"PartsSupplier    PartsSupplier    PARTSSUPPLIER.SHIP_VIA    Table    SHIP_VIA    true    true    mmuuid:0f4e9b80-73ff-1edc-a81c-ecf397b10590    4    null    false    false    2",
    			"PartsSupplier    PartsSupplier    PARTSSUPPLIER.STATUS    Table    STATUS    true    true    mmuuid:1f297200-73ff-1edc-a81c-ecf397b10590    3    null    false    false    3",
    			"PartsSupplier    PartsSupplier    PARTSSUPPLIER.SUPPLIER_PARTS    Table    SUPPLIER_PARTS    true    true    mmuuid:3deafb00-73ff-1edc-a81c-ecf397b10590    227    null    false    false    4",
    			"PartsSupplier    PartsSupplier    PARTSSUPPLIER.SUPPLIER    Table    SUPPLIER    true    true    mmuuid:2c371ec0-73ff-1edc-a81c-ecf397b10590    16    null    false    false    5"
    	};
       executeTest("select * from tables where schemaname ='PartsSupplier'", expected); //$NON-NLS-1$

       String[] expectedBefore = {
    		   "VDBName[string]    SchemaName[string]    Name[string]    Type[string]    NameInSource[string]    IsPhysical[boolean]    SupportsUpdates[boolean]    UID[string]    Cardinality[integer]    Description[string]    IsSystem[boolean]    IsMaterialized[boolean]    OID[integer]",
       };
       String[] expectedAfter = {
    		   "VDBName[string]    SchemaName[string]    Name[string]    Type[string]    NameInSource[string]    IsPhysical[boolean]    SupportsUpdates[boolean]    UID[string]    Cardinality[integer]    Description[string]    IsSystem[boolean]    IsMaterialized[boolean]    OID[integer]",
    		   "PartsSupplier    BQT1    HugeA    Table    null    true    false    mmuuid:7c66fc80-33d2-1dfa-9931-e83d04ce10a0    500000    null    false    false    6",
    		   "PartsSupplier    BQT1    HugeB    Table    null    true    false    mmuuid:b0369400-33f8-1dfa-9931-e83d04ce10a0    500000    null    false    false    7",
    		   "PartsSupplier    BQT1    LargeA    Table    null    true    false    mmuuid:3976a800-33b2-1dfa-9931-e83d04ce10a0    10000    null    false    false    8",
    		   "PartsSupplier    BQT1    LargeB    Table    null    true    false    mmuuid:5fb40600-33c3-1dfa-9931-e83d04ce10a0    10000    null    false    false    9",
    		   "PartsSupplier    BQT1    MediumA    Table    null    true    false    mmuuid:61074980-338d-1dfa-9931-e83d04ce10a0    1000    null    false    false    10",
    		   "PartsSupplier    BQT1    MediumB    Table    null    true    false    mmuuid:e24bd1c0-33a4-1dfa-9931-e83d04ce10a0    1000    null    false    false    11",
    		   "PartsSupplier    BQT1    SmallA    Table    null    true    false    mmuuid:0968424f-e6a0-1df9-ac06-b890ff96f710    50    null    false    false    12",
    		   "PartsSupplier    BQT1    SmallB    Table    null    true    false    mmuuid:06fb8980-3377-1dfa-9931-e83d04ce10a0    50    null    false    false    13"      
       };

       executeTest("select * from tables where schemaname='BQT1'", expectedBefore); //$NON-NLS-1$
       
       this.internalConnection.close();
       
       server.deployVDB(VDB2, UnitTestUtil.getTestDataPath()+"/QT_Ora9DS_1.vdb");
       
       server.mergeVDBS(VDB2, VDB1);
       
       this.internalConnection = server.createConnection("jdbc:teiid:"+VDB1);
       executeTest("select * from tables where schemaname='BQT1'", expectedAfter); //$NON-NLS-1$
       
       server.undeployVDB(VDB2);
       
       // since the connection is not closed; need to behave as if still merged
       executeTest("select * from tables where schemaname='BQT1'", expectedAfter); //$NON-NLS-1$
       
       // re-connect should behave as the original
       this.internalConnection.close();
       this.internalConnection = server.createConnection("jdbc:teiid:"+VDB1);
       
       executeTest("select * from tables where schemaname='BQT1'", expectedBefore); //$NON-NLS-1$
       executeTest("select * from tables where schemaname ='PartsSupplier'", expected); //$NON-NLS-1$
    }
	
    private void executeTest(String sql, String[] expected){
    	execute(sql);
    	if (expected != null) {
    		assertResults(expected);
    	} else {
    		printResults(true);
    	}
    }	
    
    @Test
    public void testMergeWithEmptyVDB() throws Exception {
		server.deployVDB("empty", UnitTestUtil.getTestDataPath() + "/empty.vdb");
    	this.internalConnection = server.createConnection("jdbc:teiid:empty");
    
        String[] expectedBefore = {
     		   "VDBName[string]    SchemaName[string]    Name[string]    Type[string]    NameInSource[string]    IsPhysical[boolean]    SupportsUpdates[boolean]    UID[string]    Cardinality[integer]    Description[string]    IsSystem[boolean]    IsMaterialized[boolean]    OID[integer]",
        };
    	
    	executeTest("select * from tables where schemaname ='BQT1'", expectedBefore); //$NON-NLS-1$
	
        this.internalConnection.close();
        
        server.deployVDB(VDB2, UnitTestUtil.getTestDataPath()+"/QT_Ora9DS_1.vdb");
        
        server.mergeVDBS(VDB2, "empty");

        String[] expectedAfter = {
        		"VDBName[string]    SchemaName[string]    Name[string]    Type[string]    NameInSource[string]    IsPhysical[boolean]    SupportsUpdates[boolean]    UID[string]    Cardinality[integer]    Description[string]    IsSystem[boolean]    IsMaterialized[boolean]    OID[integer]",
        		"empty    BQT1    HugeA    Table    null    true    false    mmuuid:7c66fc80-33d2-1dfa-9931-e83d04ce10a0    500000    null    false    false    1",
        		"empty    BQT1    HugeB    Table    null    true    false    mmuuid:b0369400-33f8-1dfa-9931-e83d04ce10a0    500000    null    false    false    2",
        		"empty    BQT1    LargeA    Table    null    true    false    mmuuid:3976a800-33b2-1dfa-9931-e83d04ce10a0    10000    null    false    false    3",
        		"empty    BQT1    LargeB    Table    null    true    false    mmuuid:5fb40600-33c3-1dfa-9931-e83d04ce10a0    10000    null    false    false    4",
        		"empty    BQT1    MediumA    Table    null    true    false    mmuuid:61074980-338d-1dfa-9931-e83d04ce10a0    1000    null    false    false    5",
        		"empty    BQT1    MediumB    Table    null    true    false    mmuuid:e24bd1c0-33a4-1dfa-9931-e83d04ce10a0    1000    null    false    false    6",
        		"empty    BQT1    SmallA    Table    null    true    false    mmuuid:0968424f-e6a0-1df9-ac06-b890ff96f710    50    null    false    false    7",
        		"empty    BQT1    SmallB    Table    null    true    false    mmuuid:06fb8980-3377-1dfa-9931-e83d04ce10a0    50    null    false    false    8"        		
        };
        
        this.internalConnection = server.createConnection("jdbc:teiid:empty");
        executeTest("select * from tables where schemaname='BQT1'", expectedAfter); //$NON-NLS-1$
    	
    }
}
