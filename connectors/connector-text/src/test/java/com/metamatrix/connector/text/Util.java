/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.connector.text;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import com.metamatrix.cdk.api.ConnectorHost;
import com.metamatrix.cdk.unittest.FakeTranslationFactory;
import com.metamatrix.core.util.UnitTestUtil;

public class Util {

	static void helpTestExecution(String vdb, String descriptorFile, String sql, int maxBatchSize, int expectedRowCount) throws Exception {
		descriptorFile = UnitTestUtil.getTestDataPath() + File.separator + descriptorFile;
		Properties connProps = new Properties();
		connProps.load(new FileInputStream(descriptorFile));
	    connProps.put(TextPropertyNames.DESCRIPTOR_FILE, descriptorFile);
	    connProps.put(TextPropertyNames.DATE_RESULT_FORMATS, "yyyy-MM-dd,hh:mm:ss,hh:mm,dd/mm/yyyy"); //$NON-NLS-1$
	    connProps.put(TextPropertyNames.DATE_RESULT_FORMATS_DELIMITER, ","); //$NON-NLS-1$
	    ConnectorHost host = new ConnectorHost(new TextConnector(), connProps, UnitTestUtil.getTestDataPath() + File.separator + vdb, false);
	    List results = host.executeCommand(sql);
	    Assert.assertEquals("Total row count doesn't match expected size. ", expectedRowCount, results.size()); //$NON-NLS-1$
	}

	public static ConnectorHost getConnectorHostWithFakeMetadata(String descriptorFile) throws Exception {
		Properties connProps = new Properties();
		connProps.load(new FileInputStream(descriptorFile));
	    connProps.put(TextPropertyNames.DESCRIPTOR_FILE, descriptorFile);
	    connProps.put(TextPropertyNames.COLUMN_CNT_MUST_MATCH_MODEL, "true");
	    connProps.put(TextPropertyNames.DATE_RESULT_FORMATS, "yyyy-MM-dd,hh:mm:ss,hh:mm,dd/mm/yyyy"); //$NON-NLS-1$
	    connProps.put(TextPropertyNames.DATE_RESULT_FORMATS_DELIMITER, ","); //$NON-NLS-1$
	    ConnectorHost host = new ConnectorHost(new TextConnector(), connProps, FakeTranslationFactory.getInstance().getTextTranslationUtility(), false);
	    return host;
	}

}
