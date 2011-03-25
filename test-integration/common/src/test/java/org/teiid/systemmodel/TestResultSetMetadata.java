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

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;


@SuppressWarnings("nls")
public class TestResultSetMetadata extends AbstractMMQueryTestCase {

    private static final String VDB = "PartsSupplier"; //$NON-NLS-1$
    
	public TestResultSetMetadata() {
		// this is needed because the result files are generated
		// with another tool which uses tab as delimiter
		super.DELIMITER = "\t"; //$NON-NLS-1$
	}
    
    @Before public void setUp() throws Exception {
    	FakeServer server = new FakeServer();
    	server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
    	this.internalConnection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$	    	
    }
    
    @After public void tearDown() throws SQLException {
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
    		    "count	4	integer	java.lang.Integer	1	null	null	PartsSupplier" //$NON-NLS-1$
    	};
    	executeTest("select count(*) from parts where 1=0", expected); //$NON-NLS-1$
    }

    @Test public void testStar()  throws Exception {
    	String[] expected = {
		    "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",	 //$NON-NLS-1$
		    "PART_ID	12	string	java.lang.String	0	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier", //$NON-NLS-1$
		    "PART_NAME	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier", //$NON-NLS-1$
		    "PART_COLOR	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier", //$NON-NLS-1$
		    "PART_WEIGHT	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier" //$NON-NLS-1$
    	};
    	executeTest("select * from parts where 1=0", expected); //$NON-NLS-1$
    }

    @Test public void testTempGroupStar()  throws Exception {
    	String[] expected = { 
		    "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",	 //$NON-NLS-1$
		    "PART_ID	12	string	java.lang.String	0	FOO	null	PartsSupplier", //$NON-NLS-1$
		    "PART_NAME	12	string	java.lang.String	1	FOO	null	PartsSupplier", //$NON-NLS-1$
		    "PART_COLOR	12	string	java.lang.String	1	FOO	null	PartsSupplier", //$NON-NLS-1$
		    "PART_WEIGHT	12	string	java.lang.String	1	FOO	null	PartsSupplier" //$NON-NLS-1$
    	};
    	executeTest("select * from (select * from parts) foo where 1=0", expected); //$NON-NLS-1$
    }

    @Test public void testCountAndElement()  throws Exception {
    	String[] expected = {
	        "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName",	 //$NON-NLS-1$
	        "count	4	integer	java.lang.Integer	1	null	null	PartsSupplier", //$NON-NLS-1$
	        "PART_NAME	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier" //$NON-NLS-1$
    	};
    	executeTest("select count(*), part_name from parts where 1=0 group by part_name", expected); //$NON-NLS-1$
    }

    @Test public void testStar_PreparedStatement()  throws Exception {
    	String[] expected = {
	        "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName", //$NON-NLS-1$
	        "PART_ID	12	string	java.lang.String	0	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier", //$NON-NLS-1$
	        "PART_NAME	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier", //$NON-NLS-1$
	        "PART_COLOR	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier", //$NON-NLS-1$
	        "PART_WEIGHT	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier" //$NON-NLS-1$
    	};    	
    	executePreparedTest("select * from parts where 1=0", expected); //$NON-NLS-1$
    }

    @Test public void testCount_PreparedStatement()  throws Exception {
    	String[] expected = {
	        "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName", //$NON-NLS-1$
	        "count	4	integer	java.lang.Integer	1	null	null	PartsSupplier" //$NON-NLS-1$
    	};
    	executePreparedTest("select count(*) from parts where 1=0", expected); //$NON-NLS-1$
    }

    @Test public void testCountAndElement_PreparedStatement()  throws Exception {
    	String[] expected = {
	        "ColumnName	ColumnType	ColumnTypeName	ColumnClassName	isNullable	TableName	SchemaName	CatalogName", //$NON-NLS-1$
	        "count	4	integer	java.lang.Integer	1	null	null	PartsSupplier", //$NON-NLS-1$
	        "PART_NAME	12	string	java.lang.String	1	PARTSSUPPLIER.PARTS	PartsSupplier	PartsSupplier" //$NON-NLS-1$
    	};
    	executePreparedTest("select count(*), part_name from parts where 1=0 group by part_name", expected); //$NON-NLS-1$
    }
}
