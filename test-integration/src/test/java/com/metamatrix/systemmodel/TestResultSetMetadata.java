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

import junit.framework.Test;
import junit.framework.TestSuite;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;

public class TestResultSetMetadata extends AbstractMMQueryTestCase {

    private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath() + "/partssupplier/dqp.properties"; //$NON-NLS-1$
    private static final String VDB = "PartsSupplier"; //$NON-NLS-1$
    
    public TestResultSetMetadata() {
    	// this is needed because the result files are generated 
    	// with another tool which uses tab as delimiter 
    	super.DELIMITER = "\t";
    }
    
    public static Test suite() {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(TestResultSetMetadata.class);
		return createOnceRunSuite(suite, new ConnectionFactory() {

			public com.metamatrix.jdbc.api.Connection createSingleConnection() throws Exception {
				// TODO Auto-generated method stub
				return createConnection(VDB, DQP_PROP_FILE, "");
			}});
	} 
    
    private void executeTest(String sql, String[] expected) throws Exception {
    	java.sql.ResultSet rs = execute(sql);
    	assertResultsSetMetadataEquals(rs.getMetaData(), expected);
    }
    
    private void executePreparedTest(String sql, String[] expected) throws Exception {
    	java.sql.ResultSet rs = executePreparedStatement(sql, new Object[] {});
    	assertResultsSetMetadataEquals(rs.getMetaData(), expected);
    }
    
    public void testCount()  throws Exception {
    	String[] expected = {
    		    "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",	
    		    "count	4	integer	java.lang.Integer	1	null	PartsSupplier	null"
    	};
    	executeTest("select count(*) from parts", expected);
    }

    public void testStar()  throws Exception {
    	String[] expected = {
		    "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",	
		    "PART_ID	12	string	java.lang.String	0	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null",
		    "PART_NAME	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null",
		    "PART_COLOR	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null",
		    "PART_WEIGHT	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null"
    	};
    	executeTest("select * from parts", expected);
    }

    public void testTempGroupStar()  throws Exception {
    	String[] expected = { 
		    "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",	
		    "PART_ID	12	string	java.lang.String	0	FOO	PartsSupplier	null",
		    "PART_NAME	12	string	java.lang.String	1	FOO	PartsSupplier	null",
		    "PART_COLOR	12	string	java.lang.String	1	FOO	PartsSupplier	null",
		    "PART_WEIGHT	12	string	java.lang.String	1	FOO	PartsSupplier	null"
    	};
    	executeTest("select * from (select * from parts) foo", expected);
    }

    public void testCountAndElement()  throws Exception {
    	String[] expected = {
	        "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",	
	        "count	4	integer	java.lang.Integer	1	null	PartsSupplier	null",
	        "part_name	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null"
    	};
    	executeTest("select count(*), part_name from parts group by part_name", expected);
    }

    public void testStar_PreparedStatement()  throws Exception {
    	String[] expected = {
	        "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",
	        "PART_ID	12	string	java.lang.String	0	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null",
	        "PART_NAME	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null",
	        "PART_COLOR	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null",
	        "PART_WEIGHT	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null"
    	};    	
    	executePreparedTest("select * from parts", expected);
    }

    public void testCount_PreparedStatement()  throws Exception {
    	String[] expected = {
	        "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",
	        "count	4	integer	java.lang.Integer	1	null	PartsSupplier	null"
    	};
    	executePreparedTest("select count(*) from parts", expected);
    }

    public void testCountAndElement_PreparedStatement()  throws Exception {
    	String[] expected = {
	        "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",
	        "count	4	integer	java.lang.Integer	1	null	PartsSupplier	null",
	        "part_name	12	string	java.lang.String	1	PartsSupplier.PARTSSUPPLIER.PARTS	PartsSupplier	null"
    	};
    	executePreparedTest("select count(*), part_name from parts group by part_name", expected);
    }
}
