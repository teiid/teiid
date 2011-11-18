package org.teiid.adminapi.impl;

import java.io.FileInputStream;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;

@SuppressWarnings("nls")
public class TestVDBMetadataParser {

	@Test
	public void testparseVDB() throws Exception {
		FileInputStream in = new FileInputStream(UnitTestUtil.getTestDataPath() + "/parser-test-vdb.xml");
		VDBMetaData vdb = VDBMetadataParser.unmarshell(in);
		TestVDBMetaData.validateVDB(vdb);
	}
}
