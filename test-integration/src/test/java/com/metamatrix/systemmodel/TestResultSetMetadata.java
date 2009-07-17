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

package com.metamatrix.systemmodel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;

public class TestResultSetMetadata extends AbstractMMQueryTestCase {

    private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath() + "/partssupplier/dqp.properties;user=test"; //$NON-NLS-1$
    private static final String VDB = "PartsSupplier"; //$NON-NLS-1$
    
    public TestResultSetMetadata() {
    	// this is needed because the result files are generated 
    	// with another tool which uses tab as delimiter 
    	super.DELIMITER = "\t"; //$NON-NLS-1$
    }
    
    @Before public void setUp() {
    	getConnection(VDB, DQP_PROP_FILE);
    }
    
    @After public void tearDown() {
    	closeConnection();
    }
    
    private void executeTest(String sql, String[] expected) throws Exception {
    	execute(sql);
    	java.sql.ResultSet rs = this.internalResultSet;
    	assertResultsSetMetadataEquals(rs.getMetaData(), expected);
    }
    
    private void executePreparedTest(String sql, String[] expected) throws Exception {
    	execute(sql, new Object[] {});
    	java.sql.ResultSet rs = this.internalResultSet;
    	assertResultsSetMetadataEquals(rs.getMetaData(), expected);
    }
    
    @Test public void testCount()  throws Exception {
    	String[] expected = {
    		    "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",	 //$NON-NLS-1$
    		    "count	4	integer	java.lang.Integer	1	null	PartsSupplier	null" //$NON-NLS-1$
    	};
    	executeTest("select count(*) from parts", expected); //$NON-NLS-1$
    }

    @Test public void testStar()  throws Exception {
    	String[] expected = {
		    "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",	 //$NON-NLS-1$
		    "PART_ID	12	string	java.lang.String	0	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null", //$NON-NLS-1$
		    "PART_NAME	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null", //$NON-NLS-1$
		    "PART_COLOR	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null", //$NON-NLS-1$
		    "PART_WEIGHT	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null" //$NON-NLS-1$
    	};
    	executeTest("select * from parts", expected); //$NON-NLS-1$
    }

    @Test public void testTempGroupStar()  throws Exception {
    	String[] expected = { 
		    "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",	 //$NON-NLS-1$
		    "PART_ID	12	string	java.lang.String	0	FOO	PartsSupplier	null", //$NON-NLS-1$
		    "PART_NAME	12	string	java.lang.String	1	FOO	PartsSupplier	null", //$NON-NLS-1$
		    "PART_COLOR	12	string	java.lang.String	1	FOO	PartsSupplier	null", //$NON-NLS-1$
		    "PART_WEIGHT	12	string	java.lang.String	1	FOO	PartsSupplier	null" //$NON-NLS-1$
    	};
    	executeTest("select * from (select * from parts) foo", expected); //$NON-NLS-1$
    }

    @Test public void testCountAndElement()  throws Exception {
    	String[] expected = {
	        "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",	 //$NON-NLS-1$
	        "count	4	integer	java.lang.Integer	1	null	PartsSupplier	null", //$NON-NLS-1$
	        "part_name	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null" //$NON-NLS-1$
    	};
    	executeTest("select count(*), part_name from parts group by part_name", expected); //$NON-NLS-1$
    }

    @Test public void testStar_PreparedStatement()  throws Exception {
    	String[] expected = {
	        "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName", //$NON-NLS-1$
	        "PART_ID	12	string	java.lang.String	0	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null", //$NON-NLS-1$
	        "PART_NAME	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null", //$NON-NLS-1$
	        "PART_COLOR	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null", //$NON-NLS-1$
	        "PART_WEIGHT	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null" //$NON-NLS-1$
    	};    	
    	executePreparedTest("select * from parts", expected); //$NON-NLS-1$
    }

    @Test public void testCount_PreparedStatement()  throws Exception {
    	String[] expected = {
	        "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName", //$NON-NLS-1$
	        "count	4	integer	java.lang.Integer	1	null	PartsSupplier	null" //$NON-NLS-1$
    	};
    	executePreparedTest("select count(*) from parts", expected); //$NON-NLS-1$
    }

    @Test public void testCountAndElement_PreparedStatement()  throws Exception {
    	String[] expected = {
	        "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName", //$NON-NLS-1$
	        "count	4	integer	java.lang.Integer	1	null	PartsSupplier	null", //$NON-NLS-1$
	        "part_name	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null" //$NON-NLS-1$
    	};
    	executePreparedTest("select count(*), part_name from parts group by part_name", expected); //$NON-NLS-1$
    }
}
