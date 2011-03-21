package org.teiid.jdbc;

import org.teiid.core.util.UnitTestUtil;

public class MetadataSizeExperiment {

	static FakeServer server = new FakeServer();

	public static void main(String[] args) throws Exception {
    	server.deployVDB("test", UnitTestUtil.getTestDataPath() + "/TestCase3473/test.vdb");
    	server.deployVDB("QT_Ora9DS", UnitTestUtil.getTestDataPath()+"/QT_Ora9DS_1.vdb");
		server.deployVDB("x", UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
		server.deployVDB("x1", UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
		
		Thread.sleep(10000000);
		
		System.out.println(server);
	}
	
}
