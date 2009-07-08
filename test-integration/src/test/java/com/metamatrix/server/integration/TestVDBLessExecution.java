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

package com.metamatrix.server.integration;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.junit.Test;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;

/**
 * Test the DatabaseMetadata results using the Parts VDB.
 */
public class TestVDBLessExecution extends AbstractMMQueryTestCase {

    private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath() + "/vdbless/dqp.properties;user=test"; //$NON-NLS-1$
    private static final String VDB = "VDBLess"; //$NON-NLS-1$

    @Test public void testExecution() {
    	getConnection(VDB, DQP_PROP_FILE);
    	executeAndAssertResults("select * from Example", new String[] { //$NON-NLS-1$
    			"TRADEID[string]    NOTIONAL[integer]", //$NON-NLS-1$
    			                            "x    1",  //$NON-NLS-1$
    			                            "y    2"   //$NON-NLS-1$

    	});
    }
    
    @Test public void testDatabaseMetaDataTables() throws Exception {
    	Connection conn = getConnection(VDB, DQP_PROP_FILE);
    	DatabaseMetaData metadata = conn.getMetaData();
    	this.internalResultSet = metadata.getTables(null, null, "%", new String[] {"TABLE"}); //$NON-NLS-1$ //$NON-NLS-2$
    	assertResults(new String[] {
    			"TABLE_CAT[string]    TABLE_SCHEM[string]    TABLE_NAME[string]    TABLE_TYPE[string]    REMARKS[string]    TYPE_CAT[string]    TYPE_SCHEM[string]    TYPE_NAME[string]    SELF_REFERENCING_COL_NAME[string]    REF_GENERATION[string]    ISPHYSICAL[boolean]", //$NON-NLS-1$
    			"null    VDBLess    SummitData.EXAMPLE    TABLE    null    null    null    null    null    null    true" //$NON-NLS-1$
    	});
    }
    
    @Test public void testDatabaseMetaDataColumns() throws Exception {
    	Connection conn = getConnection(VDB, DQP_PROP_FILE);
    	DatabaseMetaData metadata = conn.getMetaData();
    	this.internalResultSet = metadata.getColumns(null, null, "SummitData%", "%"); //$NON-NLS-1$ //$NON-NLS-2$
    	assertResults(new String[] {
    			"TABLE_CAT[string]    TABLE_SCHEM[string]    TABLE_NAME[string]    COLUMN_NAME[string]    DATA_TYPE[short]    TYPE_NAME[string]    COLUMN_SIZE[integer]    BUFFER_LENGTH[string]    DECIMAL_DIGITS[integer]    NUM_PREC_RADIX[integer]    NULLABLE[integer]    REMARKS[string]    COLUMN_DEF[string]    SQL_DATA_TYPE[string]    SQL_DATETIME_SUB[string]    CHAR_OCTET_LENGTH[integer]    ORDINAL_POSITION[integer]    IS_NULLABLE[string]", //$NON-NLS-1$
    			"null    VDBLess    SummitData.EXAMPLE    TRADEID    12    string    4000    null    0    0    0    null    null    null    null    0    1    YES", //$NON-NLS-1$
    			"null    VDBLess    SummitData.EXAMPLE    NOTIONAL    4    integer    10    null    0    0    0    null    null    null    null    0    2    YES" //$NON-NLS-1$
    	});
    }
        
}
