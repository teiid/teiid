/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.systemmodel;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;

/**
 * Test the DatabaseMetadata results using the Parts VDB.
 */
public class TestPartsDatabaseMetadata extends AbstractMMQueryTestCase {

    private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath() + "/partssupplier/dqp.properties"; //$NON-NLS-1$
    private static final String VDB = "PartsSupplier"; //$NON-NLS-1$

    static DatabaseMetaData dbMetadata;
    static Connection connection;
    
    public TestPartsDatabaseMetadata() {
    	// this is needed because the result files are generated 
    	// with another tool which uses tab as delimiter 
    	super.DELIMITER = "\t";
    }
    
    public static Test suite() {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(TestPartsDatabaseMetadata.class);
		return createOnceRunSuite(suite, new ConnectionFactory() {

			public com.metamatrix.jdbc.api.Connection createSingleConnection()
					throws Exception {
				com.metamatrix.jdbc.api.Connection result = createConnection(VDB, DQP_PROP_FILE, "");
				dbMetadata = result.getMetaData();
				return result;
			}});
	}    
    
    private void checkResult(String testName, ResultSet actualResults)  throws Exception {
    	ResultSetMetaData resultMetadata = actualResults.getMetaData();
        
    	String metafilename = UnitTestUtil.getTestDataPath() + File.separator+"partssupplier"+File.separator + "expected" + File.separator+ testName.substring(4) + ".metadata.txt"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$        
        assertResultsSetMetadataEquals(resultMetadata, new File(metafilename));
    	
        String filename = UnitTestUtil.getTestDataPath() + File.separator+"partssupplier"+File.separator + "expected" + File.separator+ testName.substring(4) + ".txt"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$        
        assertResultsSetEquals(actualResults, new File(filename));
    }
        
    public void testExportedKeys()  throws Exception {
        checkResult("testExportedKeys", dbMetadata.getExportedKeys(null, VDB, "%")); //$NON-NLS-1$
    }

    public void testImportedKeys()  throws Exception {
        checkResult("testImportedKeys", dbMetadata.getImportedKeys(null, VDB, "%")); //$NON-NLS-1$
    }

    public void testPrimaryKeys()  throws Exception {
        checkResult("testPrimaryKeys", dbMetadata.getPrimaryKeys(null, VDB, "%")); //$NON-NLS-1$
    }

    public void testProcedures()  throws Exception {
        checkResult("testProcedures", dbMetadata.getProcedures(null, VDB, "%")); //$NON-NLS-1$
    }

    public void testProcedureColumns()  throws Exception {
        checkResult("testProcedureColumns", dbMetadata.getProcedureColumns(null, VDB, "%", "%")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testUDTs()  throws Exception {
        checkResult("testUDTs", dbMetadata.getUDTs(null, VDB, "%", null)); //$NON-NLS-1$
    }

    public void testIndexInfo()  throws Exception {
        checkResult("testIndexInfo", dbMetadata.getIndexInfo(null, VDB, "%", true, true)); //$NON-NLS-1$
    }

    public void testCrossReference()  throws Exception {
        checkResult("testCrossReference", dbMetadata.getCrossReference(null, VDB, "%", null, VDB, "%")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTypeInfo()  throws Exception {
        checkResult("testTypeInfo",dbMetadata.getTypeInfo());
    }
    
    public void testCatalogs()  throws Exception {
        checkResult("testCatalogs", dbMetadata.getCatalogs());
    }
    
    public void testSchemas()  throws Exception {
        checkResult("testSchemas", dbMetadata.getSchemas());
    }

    public void testTables()  throws Exception {
        checkResult("testTables", dbMetadata.getTables(null,VDB, "%", null)); //$NON-NLS-1$
    }

    public void testColumns() throws Exception {
        checkResult("testColumns", dbMetadata.getColumns(null, VDB, "%", "%")); //$NON-NLS-1$ //$NON-NLS-2$
    }    
}
