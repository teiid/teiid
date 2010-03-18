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

package org.teiid.systemmodel;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.jdbc.api.AbstractMMQueryTestCase;

import com.metamatrix.core.util.UnitTestUtil;

/**
 * Test the DatabaseMetadata results using the Parts VDB.
 */
public class TestPartsDatabaseMetadata extends AbstractMMQueryTestCase {

    private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath() + "/partssupplier/dqp.properties;user=test"; //$NON-NLS-1$
    private static final String VDB = "PartsSupplier"; //$NON-NLS-1$

    static DatabaseMetaData dbMetadata;
    static Connection connection;
    
    public TestPartsDatabaseMetadata() {
    	// this is needed because the result files are generated 
    	// with another tool which uses tab as delimiter 
    	super.DELIMITER = "\t"; //$NON-NLS-1$
    }
    
    @Before public void setUp() throws SQLException {
    	getConnection(VDB, DQP_PROP_FILE);
    	dbMetadata = this.internalConnection.getMetaData();
    }
    
    @After public void tearDown() {
    	closeConnection();
    }
    
    @Test public void testExportedKeys()  throws Exception {
        checkResult("testExportedKeys", dbMetadata.getExportedKeys(VDB, null, "%")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    private void checkResult(String string, ResultSet exportedKeys) throws Exception {
    	checkResult(string, exportedKeys, "partssupplier"); //$NON-NLS-1$
	}

	@Test public void testImportedKeys()  throws Exception {
        checkResult("testImportedKeys", dbMetadata.getImportedKeys(VDB, null, "%")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testPrimaryKeys()  throws Exception {
        checkResult("testPrimaryKeys", dbMetadata.getPrimaryKeys(VDB, null, "%")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testProcedures()  throws Exception {
        checkResult("testProcedures", dbMetadata.getProcedures(VDB, null, "%")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testProcedureColumns()  throws Exception {
        checkResult("testProcedureColumns", dbMetadata.getProcedureColumns(VDB, null, "%", "%")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testUDTs()  throws Exception {
        checkResult("testUDTs", dbMetadata.getUDTs(VDB, null, "%", null)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testIndexInfo()  throws Exception {
        checkResult("testIndexInfo", dbMetadata.getIndexInfo(VDB, null, "%", true, true)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testCrossReference()  throws Exception {
        checkResult("testCrossReference", dbMetadata.getCrossReference(VDB, null, "%", VDB, null, "%")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testTypeInfo()  throws Exception {
        checkResult("testTypeInfo",dbMetadata.getTypeInfo()); //$NON-NLS-1$
    }
    
    @Test public void testCatalogs()  throws Exception {
        checkResult("testCatalogs", dbMetadata.getCatalogs()); //$NON-NLS-1$
    }
    
    @Test public void testSchemas()  throws Exception {
        checkResult("testSchemas", dbMetadata.getSchemas()); //$NON-NLS-1$
    }

    @Test public void testTables()  throws Exception {
        checkResult("testTables", dbMetadata.getTables(VDB, null, "%", null)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testColumns() throws Exception {
        checkResult("testColumns", dbMetadata.getColumns(VDB, null, "%", "%")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }    
}
