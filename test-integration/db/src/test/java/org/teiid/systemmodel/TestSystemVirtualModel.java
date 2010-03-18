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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.jdbc.api.AbstractMMQueryTestCase;

import com.metamatrix.core.util.UnitTestUtil;

/**
 * Exercises each virtual table in the system model.
 */
public class TestSystemVirtualModel extends AbstractMMQueryTestCase {
	private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath()+ "/partssupplier/dqp.properties;user=test"; //$NON-NLS-1$
	private static final String VDB = "PartsSupplier"; //$NON-NLS-1$

	public TestSystemVirtualModel() {
		// this is needed because the result files are generated
		// with another tool which uses tab as delimiter
		super.DELIMITER = "\t"; //$NON-NLS-1$
	}
	
    @Before public void setUp() {
    	getConnection(VDB, DQP_PROP_FILE);
    }

    @Ignore("ODBC support to be readded")
	@Test public void testDefect23534() {
		String[] expected = { "SCOPE[short]	COLUMN_NAME[string]	DATA_TYPE[short]	TYPE_NAME[string]	PRECISION[integer]	LENGTH[integer]	SCALE[integer]	PSEUDO_COLUMN[short]" }; //$NON-NLS-1$
		executeAndAssertResults(
				"SELECT OA_SCOPE as \"SCOPE\" , COLUMN_NAME,  DATA_TYPE, TYPE_NAME, OA_PRECISION as \"PRECISION\", " + //$NON-NLS-1$
				"OA_LENGTH as \"LENGTH\", OA_SCALE as \"SCALE\", PSEUDO_COLUMN " + //$NON-NLS-1$
				"FROM System.ODBC.OA_COLUMNS  " + //$NON-NLS-1$
				"WHERE TABLE_NAME = N'AUTHORS' AND TABLE_OWNER = N's1' AND " + //$NON-NLS-1$
				"(OA_COLUMNTYPE = 2 OR OA_COLUMNTYPE = 3) " + //$NON-NLS-1$
				"AND (OA_SCOPE is null OR OA_SCOPE >= 1) " + //$NON-NLS-1$
				"AND (OA_NULLABLE = 0 OR OA_NULLABLE = 1) " + //$NON-NLS-1$
				"ORDER BY \"SCOPE\"", //$NON-NLS-1$
				expected);
	}
    
    protected void checkResult(String testName, String query) throws Exception {
    	execute(query);
    	super.checkResult(testName, this.internalResultSet, "system"); //$NON-NLS-1$
    }
    
	@Test public void testModels() throws Exception {
		checkResult("testSchemas", "select* from System.Schemas"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testKeys() throws Exception {
		checkResult("testKeys", "select* from System.Keys order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testGroups() throws Exception {
		checkResult("testTables", "select* from System.Tables order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testDataTypes() throws Exception {
		checkResult("testDataTypes", "select * from System.DataTypes order by name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testProcedureParams() throws Exception {
		checkResult("testProcedureParams", "select * from System.ProcedureParams order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testProcedures() throws Exception {
		checkResult("testProcedures", "select* from System.Procedures order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testProperties() {
		String[] expected = { "Name[string]	Value[string]	UID[string]", }; //$NON-NLS-1$
		executeAndAssertResults("select* from System.Properties", expected); //$NON-NLS-1$
	}

	@Test public void testVirtualDatabase() {

		String[] expected = { "Name[string]	Version[string]	", "PartsSupplier	1", //$NON-NLS-1$ //$NON-NLS-2$

		};
		executeAndAssertResults("select* from System.VirtualDatabases", //$NON-NLS-1$
				expected);
	}

	@Test public void testKeyColumns() throws Exception {
		checkResult("testKeyColumns", "select* from System.KeyColumns order by Name, KeyName"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testVDBResourcePathsProcedure() {

		String[] expected = { "ResourcePath[string]	isBinary[boolean]	", //$NON-NLS-1$
				"/parts/partsmd/PartsSupplier.xmi	false", //$NON-NLS-1$
		};
		execute("exec getVDBResourcePaths()",new Object[] {}); //$NON-NLS-1$
		assertResults(expected);
	}

	@Test public void testColumns() throws Exception {
		checkResult("testColumns", "select* from System.Columns order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Test public void testTableType() {

		String[] expected = { "Type[string]	", "Table", }; //$NON-NLS-1$ //$NON-NLS-2$
		executeAndAssertResults(
				"select distinct Type from System.Tables order by Type", //$NON-NLS-1$
				expected);
	}

	@Test public void testTableIsSystem() throws Exception {
		checkResult("testTableIsSystem", "select Name from System.Tables where IsSystem = 'false' order by Name"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	@Ignore("ODBC support to be readded")
	@Test public void test_OA_PROC() {

		String[] expected = {
				"OA_QUALIFIER[string]	OA_OWNER[string]	OA_NAME[string]	NUM_INPUT_PARAMS[integer]	NUM_OUTPUT_PARAMS[integer]	NUM_RESULT_SETS[integer]	REMARKS[string]	PROCEDURE_TYPE[short]", //$NON-NLS-1$
				"PartsSupplier	System	describe	0	0	0	null	1", //$NON-NLS-1$
				"PartsSupplier	System	getBinaryVDBResource	0	0	0	null	1", //$NON-NLS-1$
				"PartsSupplier	System	getCharacterVDBResource	0	0	0	null	1", //$NON-NLS-1$
				"PartsSupplier	System	getUpdatedCharacterVDBResource	0	0	0	null	1", //$NON-NLS-1$
				"PartsSupplier	System	getVDBResourcePaths	0	0	0	null	1", //$NON-NLS-1$

		};
		executeAndAssertResults("select* FROM System.ODBC.OA_PROC", expected); //$NON-NLS-1$
	}

	@Ignore("ODBC support to be readded")
	@Test public void test_OA_PROCCOLUMNS() {

		String[] expected = {
				"OA_QUALIFIER[string]	OA_OWNER[string]	OA_NAME[string]	COLUMN_NAME[string]	OA_COLUMNTYPE[short]	DATA_TYPE[short]	TYPE_NAME[string]	OA_PRECISION[integer]	OA_LENGTH[integer]	OA_RADIX[integer]	OA_SCALE[integer]	OA_NULLABLE[short]	OA_SCOPE[short]	REMARKS[string]", //$NON-NLS-1$
				"PartsSupplier	System	describe	Description	0	12	VARCHAR	128	128	10	0	0	0	", //$NON-NLS-1$
				"PartsSupplier	System	describe	entity	0	12	VARCHAR	0	0	10	0	0	0	", //$NON-NLS-1$
				"PartsSupplier	System	getBinaryVDBResource	VdbResource	0	12	VARCHAR	0	0	10	0	1	0	", //$NON-NLS-1$
				"PartsSupplier	System	getBinaryVDBResource	resourcePath	0	12	VARCHAR	50	50	10	0	0	0	", //$NON-NLS-1$
				"PartsSupplier	System	getCharacterVDBResource	VdbResource	0	12	VARCHAR	0	0	10	0	1	0	", //$NON-NLS-1$
				"PartsSupplier	System	getCharacterVDBResource	resourcePath	0	12	VARCHAR	50	50	10	0	0	0	", //$NON-NLS-1$
				"PartsSupplier	System	getUpdatedCharacterVDBResource	VdbResource	0	12	VARCHAR	0	0	10	0	1	0	", //$NON-NLS-1$
				"PartsSupplier	System	getUpdatedCharacterVDBResource	resourcePath	0	12	VARCHAR	50	50	10	0	0	0	", //$NON-NLS-1$
				"PartsSupplier	System	getUpdatedCharacterVDBResource	tokenReplacements	0	12	VARCHAR	0	0	10	0	0	0	", //$NON-NLS-1$
				"PartsSupplier	System	getUpdatedCharacterVDBResource	tokens	0	12	VARCHAR	0	0	10	0	0	0	", //$NON-NLS-1$
				"PartsSupplier	System	getVDBResourcePaths	ResourcePath	0	12	VARCHAR	50	50	10	0	1	0	", //$NON-NLS-1$
				"PartsSupplier	System	getVDBResourcePaths	isBinary	0	5	SMALLINT	1	1	10	0	1	0", //$NON-NLS-1$

		};
		executeAndAssertResults(
				"select* FROM System.ODBC.OA_PROCCOLUMNS ORDER BY OA_NAME, COLUMN_NAME ", //$NON-NLS-1$
				expected);
	}

	@Ignore("ODBC support to be readded")
	@Test public void testOATYPES() {
		String[] expected = { "TYPE_NAME[string]	DATA_TYPE[short]	PRECISION[integer]	LITERAL_PREFIX[string]	LITERAL_SUFFIX[string]	CREATE_PARAMS[string]	NULLABLE[short]	CASE_SENSITIVE[short]	SEARCHABLE[short]	UNSIGNED_ATTRIBUTE[short]	MONEY[short]	AUTO_INCREMENT[short]	LOCAL_TYPE_NAME[string]	MINIMUM_SCALE[short]	MAXIMUM_SCALE[short]", }; //$NON-NLS-1$

		executeAndAssertResults(
				"select TYPE_NAME, DATA_TYPE, OA_PRECISION as PRECISION, LITERAL_PREFIX, LITERAL_SUFFIX, " //$NON-NLS-1$
						+ " CREATE_PARAMS, OA_NULLABLE as NULLABLE, CASE_SENSITIVE, OA_SEARCHABLE as SEARCHABLE, " //$NON-NLS-1$
						+ " UNSIGNED_ATTRIB as UNSIGNED_ATTRIBUTE, OA_MONEY as MONEY, AUTO_INCREMENT, LOCAL_TYPE_NAME, " //$NON-NLS-1$
						+ " MINIMUM_SCALE, MAXIMUM_SCALE  FROM System.ODBC.OA_TYPES  WHERE DATA_TYPE = -6", //$NON-NLS-1$
				expected);
	}

	@Ignore("ODBC support to be readded")
	@Test public void testOACOLUMNSAll() {

		String[] expected = {
				"TABLE_QUALIFIER[string]	TABLE_OWNER[string]	TABLE_NAME[string]	COLUMN_NAME[string]	DATA_TYPE[short]	TYPE_NAME[string]	OA_LENGTH[integer]	OA_PRECISION[integer]	OA_SCALE[integer]	OA_RADIX[integer]	OA_NULLABLE[short]	OA_SCOPE[short]	PSEUDO_COLUMN[short]	OA_COLUMNTYPE[short]	REMARKS[string]", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	PARTS	PART_COLOR	12	VARCHAR	30	0	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	PARTS	PART_ID	12	VARCHAR	4	0	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	PARTS	PART_NAME	12	VARCHAR	255	0	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	PARTS	PART_WEIGHT	12	VARCHAR	255	0	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SHIP_VIA	SHIPPER_ID	5	SMALLINT	0	2	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SHIP_VIA	SHIPPER_NAME	12	VARCHAR	30	0	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	STATUS	STATUS_ID	5	SMALLINT	0	2	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	STATUS	STATUS_NAME	12	VARCHAR	30	0	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER	SUPPLIER_CITY	12	VARCHAR	30	0	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER	SUPPLIER_ID	12	VARCHAR	10	0	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER	SUPPLIER_NAME	12	VARCHAR	30	0	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER	SUPPLIER_STATE	12	VARCHAR	2	0	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER	SUPPLIER_STATUS	5	SMALLINT	0	2	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER_PARTS	PART_ID	12	VARCHAR	4	0	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER_PARTS	QUANTITY	5	SMALLINT	0	3	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER_PARTS	SHIPPER_ID	5	SMALLINT	0	2	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER_PARTS	SUPPLIER_ID	12	VARCHAR	10	0	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeElementProperties	DataTypeElementName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeElementProperties	DataTypeName	12	VARCHAR	100	100	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeElementProperties	Name	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeElementProperties	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeElementProperties	Value	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeElements	DataTypeName	12	VARCHAR	100	100	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeElements	ElementLength	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeElements	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeElements	Position	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeElements	Scale	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeElements	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeProperties	DataType	12	VARCHAR	100	100	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeProperties	Name	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeProperties	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypeProperties	Value	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	BaseType	12	VARCHAR	64	64	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	Description	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	IsAutoIncremented	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	IsCaseSensitive	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	IsPhysical	5	SMALLINT	1	1	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	IsSigned	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	IsStandard	5	SMALLINT	1	1	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	JavaClass	12	VARCHAR	500	500	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	Name	12	VARCHAR	100	100	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	NullType	12	VARCHAR	20	20	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	Precision	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	Radix	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	RuntimeType	12	VARCHAR	64	64	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	Scale	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	SearchType	12	VARCHAR	20	20	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	TypeLength	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	TypeName	12	VARCHAR	100	100	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	DataTypes	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ElementProperties	ElementName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ElementProperties	ElementUpperName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ElementProperties	GroupFullName	12	VARCHAR	2048	2048	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ElementProperties	GroupName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ElementProperties	GroupUpperName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ElementProperties	ModelName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ElementProperties	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ElementProperties	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ElementProperties	Value	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	CharOctetLength	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	DataType	12	VARCHAR	100	100	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	DefaultValue	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	Description	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	ElementLength	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	Format	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	GroupFullName	12	VARCHAR	2048	2048	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	GroupName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	GroupUpperName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	IsAutoIncremented	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	IsCaseSensitive	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	IsCurrency	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	IsLengthFixed	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	IsSigned	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	JavaClass	12	VARCHAR	500	500	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	MaxRange	12	VARCHAR	50	50	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	MinRange	12	VARCHAR	50	50	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	ModelName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	NameInSource	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	NullType	12	VARCHAR	20	20	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	Position	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	Precision	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	Radix	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	Scale	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	SearchType	12	VARCHAR	20	20	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	SupportsSelect	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	SupportsUpdates	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Elements	UpperName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	GroupProperties	GroupFullName	12	VARCHAR	2048	2048	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	GroupProperties	GroupName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	GroupProperties	GroupUpperName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	GroupProperties	ModelName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	GroupProperties	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	GroupProperties	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	GroupProperties	Value	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	Cardinality	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	Description	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	FullName	12	VARCHAR	2048	2048	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	IsMaterialized	5	SMALLINT	0	0	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	IsPhysical	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	IsSystem	5	SMALLINT	1	1	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	ModelName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	NameInSource	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	SupportsUpdates	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	Type	12	VARCHAR	20	20	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Groups	UpperName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyElements	GroupFullName	12	VARCHAR	2048	2048	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyElements	GroupName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyElements	GroupUpperName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyElements	KeyName	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyElements	KeyType	12	VARCHAR	20	20	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyElements	ModelName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyElements	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyElements	Position	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyElements	RefKeyUID	12	VARCHAR	50	50	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyElements	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyProperties	GroupFullName	12	VARCHAR	2048	2048	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyProperties	GroupName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyProperties	GroupUpperName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyProperties	KeyName	12	VARCHAR	255	0	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyProperties	ModelName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyProperties	Name	12	VARCHAR	255	0	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyProperties	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	KeyProperties	Value	12	VARCHAR	255	0	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Keys	Description	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Keys	GroupFullName	12	VARCHAR	2048	2048	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Keys	GroupName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Keys	GroupUpperName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Keys	IsIndexed	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Keys	ModelName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Keys	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Keys	NameInSource	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Keys	RefKeyUID	12	VARCHAR	50	50	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Keys	Type	12	VARCHAR	20	20	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Keys	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ModelProperties	ModelName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ModelProperties	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ModelProperties	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ModelProperties	Value	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Models	Description	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Models	IsPhysical	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Models	MaxSetSize	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Models	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Models	PrimaryMetamodelURI	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Models	SupportsDistinct	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Models	SupportsJoin	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Models	SupportsOrderBy	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Models	SupportsOuterJoin	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Models	SupportsWhereAll	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Models	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureParams	DataType	12	VARCHAR	25	25	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureParams	ModelName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureParams	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureParams	NullType	12	VARCHAR	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureParams	Optional	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureParams	Position	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureParams	Precision	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureParams	ProcedureName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureParams	Radix	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureParams	Scale	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureParams	Type	12	VARCHAR	100	100	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureParams	TypeLength	4	INTEGER	10	10	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureProperties	ModelName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureProperties	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureProperties	ProcedureName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureProperties	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	ProcedureProperties	Value	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Procedures	Description	12	VARCHAR	255	225	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Procedures	FullName	12	VARCHAR	2048	2048	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Procedures	ModelName	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Procedures	ModelUID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Procedures	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Procedures	NameInSource	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Procedures	ReturnsResults	5	SMALLINT	1	1	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	Procedures	UID	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	VirtualDatabases	Name	12	VARCHAR	255	255	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System	VirtualDatabases	Version	12	VARCHAR	50	50	0	10	0	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	DEFERRABILITY	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	DELETE_RULE	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	FKCOLUMN_NAME	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	FKTABLE_CAT	12	VARCHAR	1	1	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	FKTABLE_NAME	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	FKTABLE_SCHEM	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	FK_NAME	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	KEY_SEQ	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	PKCOLUMN_NAME	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	PKTABLE_CAT	12	VARCHAR	1	1	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	PKTABLE_NAME	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	PKTABLE_SCHEM	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	PK_NAME	12	VARCHAR	255	255	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/JDBC	ReferenceKeyColumns	UPDATE_RULE	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	COLUMN_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	DATA_TYPE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	OA_COLUMNTYPE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	OA_LENGTH	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	OA_NULLABLE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	OA_PRECISION	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	OA_RADIX	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	OA_SCALE	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	OA_SCOPE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	PSEUDO_COLUMN	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	REMARKS	12	VARCHAR	254	254	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	TABLE_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	TABLE_OWNER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	TABLE_QUALIFIER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_COLUMNS	TYPE_NAME	12	VARCHAR	100	100	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	DELETE_RULE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	FKCOLUMN_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	FKTABLE_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	FKTABLE_OWNER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	FKTABLE_QUALIFIER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	FK_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	KEY_SEQ	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	PKCOLUMN_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	PKTABLE_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	PKTABLE_OWNER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	PKTABLE_QUALIFIER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	PK_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_FKEYS	UPDATE_RULE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROC	NUM_INPUT_PARAMS	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROC	NUM_OUTPUT_PARAMS	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROC	NUM_RESULT_SETS	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROC	OA_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROC	OA_OWNER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROC	OA_QUALIFIER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROC	PROCEDURE_TYPE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROC	REMARKS	12	VARCHAR	254	254	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	COLUMN_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	DATA_TYPE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	OA_COLUMNTYPE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	OA_LENGTH	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	OA_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	OA_NULLABLE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	OA_OWNER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	OA_PRECISION	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	OA_QUALIFIER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	OA_RADIX	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	OA_SCALE	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	OA_SCOPE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	REMARKS	12	VARCHAR	254	254	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_PROCCOLUMNS	TYPE_NAME	12	VARCHAR	100	100	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	COLUMN_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	FILTER_CONDITIONS	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	INDEX_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	INDEX_QUALIFIER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	NON_UNIQUE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	OA_CARDINALITY	12	VARCHAR	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	OA_COLLATION	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	OA_PAGES	12	VARCHAR	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	OA_TYPE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	SEQ_IN_INDEX	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	TABLE_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	TABLE_OWNER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_STATISTICS	TABLE_QUALIFIER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TABLES	REMARKS	12	VARCHAR	254	254	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TABLES	TABLE_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TABLES	TABLE_OWNER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TABLES	TABLE_QUALIFIER	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TABLES	TABLE_TYPE	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	AUTO_INCREMENT	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	CASE_SENSITIVE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	CREATE_PARAMS	12	VARCHAR	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	DATA_TYPE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	LITERAL_PREFIX	12	VARCHAR	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	LITERAL_SUFFIX	12	VARCHAR	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	LOCAL_TYPE_NAME	12	VARCHAR	128	128	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	MAXIMUM_SCALE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	MINIMUM_SCALE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	OA_MONEY	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	OA_NULLABLE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	OA_PRECISION	4	INTEGER	10	10	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	OA_SEARCHABLE	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	TYPE_NAME	12	VARCHAR	100	100	0	10	1	0	0	0	null", //$NON-NLS-1$
				"PartsSupplier	System/ODBC	OA_TYPES	UNSIGNED_ATTRIB	5	SMALLINT	5	5	0	10	1	0	0	0	null", //$NON-NLS-1$

		};

		executeAndAssertResults(
				"select* FROM System.ODBC.OA_COLUMNS ORDER BY TABLE_QUALIFIER, TABLE_OWNER, TABLE_NAME, COLUMN_NAME", //$NON-NLS-1$
				expected);
	}

	@Ignore("ODBC support to be readded")
	@Test public void testOATYPESAll() {
		String[] expected = {
				"TYPE_NAME[string]	DATA_TYPE[short]	OA_PRECISION[integer]	LITERAL_PREFIX[string]	LITERAL_SUFFIX[string]	CREATE_PARAMS[string]	OA_NULLABLE[short]	CASE_SENSITIVE[short]	OA_SEARCHABLE[short]	UNSIGNED_ATTRIB[short]	OA_MONEY[short]	AUTO_INCREMENT[short]	MINIMUM_SCALE[short]	MAXIMUM_SCALE[short]	LOCAL_TYPE_NAME[string]", //$NON-NLS-1$
				"CHAR	1	2147483647	'	'	null	1	1	3	0	0	0	null	null	CHAR", //$NON-NLS-1$
				"DATE	9	10	{d '	'}	null	1	0	2	0	0	0	null	null	DATE", //$NON-NLS-1$
				"DOUBLE	8	15	null	null	null	1	0	2	0	0	0	null	null	DOUBLE", //$NON-NLS-1$
				"FLOAT	6	15	null	null	null	1	0	2	0	0	0	null	null	FLOAT", //$NON-NLS-1$
				"INTEGER	4	10	null	null	null	1	0	2	0	0	0	null	null	INTEGER", //$NON-NLS-1$
				"LONGVARBINARY	-4	2147483647	0x	null	null	1	0	0	0	0	0	null	null	null", //$NON-NLS-1$
				"NUMERIC	2	32	null	null	null	1	0	2	0	0	0	0	32	NUMERIC", //$NON-NLS-1$
				"REAL	7	7	null	null	null	1	0	2	0	0	0	null	null	REAL", //$NON-NLS-1$
				"SMALLINT	5	5	null	null	null	1	0	2	0	0	0	null	null	SMALLINT", //$NON-NLS-1$
				"TIME	10	8	{t '	'}	null	1	0	2	0	0	0	null	null	TIME", //$NON-NLS-1$
				"TIMESTAMP	11	19	{ts '	'}	null	1	0	2	0	0	0	null	null	TIMESTAMP", //$NON-NLS-1$
				"VARCHAR	12	2147483647	'	'	null	1	1	3	0	0	0	null	null	VARCHAR", }; //$NON-NLS-1$
		executeAndAssertResults(
				"select* FROM System.ODBC.OA_TYPES ORDER BY TYPE_NAME", //$NON-NLS-1$
				expected);

	}

	@Ignore("ODBC support to be readded")
	@Test public void testOAFKEYS() {
		String[] expected = {
				"PKTABLE_QUALIFIER[string]	PKTABLE_OWNER[string]	PKTABLE_NAME[string]	PKCOLUMN_NAME[string]	FKTABLE_QUALIFIER[string]	FKTABLE_OWNER[string]	FKTABLE_NAME[string]	FKCOLUMN_NAME[string]	KEY_SEQ[short]	UPDATE_RULE[short]	DELETE_RULE[short]	FK_NAME[string]	PK_NAME[string]", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	PARTS	PART_ID	PartsSupplier	PartsSupplier/PARTSSUPPLIER	null	null	1	null	null	null	PK_PARTS", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	PARTS	PART_ID	PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER_PARTS	PART_ID	1	null	null	FK_SPLIER_PRTS_PRTS	PK_PARTS", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SHIP_VIA	SHIPPER_ID	PartsSupplier	PartsSupplier/PARTSSUPPLIER	null	null	1	null	null	null	PK_SHIP_VIA", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	STATUS	STATUS_ID	PartsSupplier	PartsSupplier/PARTSSUPPLIER	null	null	1	null	null	null	PK_STATUS", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	STATUS	STATUS_ID	PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER	SUPPLIER_STATUS	1	null	null	FK_SPLIER_STATS	PK_STATUS", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER	SUPPLIER_ID	PartsSupplier	PartsSupplier/PARTSSUPPLIER	null	null	1	null	null	null	PK_SUPPLIER", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER	SUPPLIER_ID	PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER_PARTS	SUPPLIER_ID	1	null	null	FK_SPLY_PRTS_SPLY	PK_SUPPLIER", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER_PARTS	SUPPLIER_ID	PartsSupplier	PartsSupplier/PARTSSUPPLIER	null	null	1	null	null	null	PK_SUPPLIER_PARTS", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER_PARTS	PART_ID	PartsSupplier	PartsSupplier/PARTSSUPPLIER	null	null	2	null	null	null	PK_SUPPLIER_PARTS", //$NON-NLS-1$

		};
		executeAndAssertResults(
				"select* FROM System.ODBC.OA_FKEYS ORDER BY PK_NAME, KEY_SEQ", //$NON-NLS-1$
				expected);

	}

	@Ignore("ODBC support to be readded")
	@Test public void testOASTATISTICS() {
		String[] expected = {
				"TABLE_QUALIFIER[string]	TABLE_OWNER[string]	TABLE_NAME[string]	NON_UNIQUE[short]	INDEX_QUALIFIER[string]	INDEX_NAME[string]	OA_TYPE[short]	SEQ_IN_INDEX[short]	COLUMN_NAME[string]	OA_COLLATION[string]	OA_CARDINALITY[string]	OA_PAGES[string]	FILTER_CONDITIONS[string]", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	PARTS	0		PK_PARTS	3	1	PART_ID	null	null	null", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SHIP_VIA	0		PK_SHIP_VIA	3	1	SHIPPER_ID	null	null	null	", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	STATUS	0		PK_STATUS	3	1	STATUS_ID	null	null	null	", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER	1		FK_SPLIER_STATS	3	1	SUPPLIER_STATUS	null	null	null	", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER	0		PK_SUPPLIER	3	1	SUPPLIER_ID	null	null	null	", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER_PARTS	1		FK_SPLIER_PRTS_PRTS	3	1	PART_ID	null	null	null	", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER_PARTS	1		FK_SPLY_PRTS_SPLY	3	1	SUPPLIER_ID	null	null	null	", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER_PARTS	0		PK_SUPPLIER_PARTS	3	2	PART_ID	null	null	null	", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier/PARTSSUPPLIER	SUPPLIER_PARTS	0		PK_SUPPLIER_PARTS	3	1	SUPPLIER_ID	null	null	null	", //$NON-NLS-1$

		};
		executeAndAssertResults("select* FROM System.ODBC.OA_STATISTICS order by TABLE_NAME, INDEX_NAME, COLUMN_NAME", //$NON-NLS-1$
				expected);

	}

	@Test public void testDefect12064() {
		String[] expected = { 
				"KeyName[string]	RefKeyUID[string]	",  //$NON-NLS-1$
				"PK_PARTS	null", //$NON-NLS-1$
				"PK_SHIP_VIA	null", //$NON-NLS-1$
				"PK_STATUS	null",  //$NON-NLS-1$
				"PK_SUPPLIER	null",  //$NON-NLS-1$
				"PK_SUPPLIER_PARTS	null", //$NON-NLS-1$
				"PK_SUPPLIER_PARTS	null",  //$NON-NLS-1$
				

		};
		executeAndAssertResults("select KeyName, RefKeyUID FROM System.KeyColumns WHERE RefKeyUID IS NULL order by KeyName",expected); //$NON-NLS-1$
	}

	@Test public void testReferenceKeyColumns() throws Exception {
		checkResult("testReferenceKeyColumns", "select* FROM System.ReferenceKeyColumns order by PKTABLE_NAME"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	@Test public void testVirtualLookup() {
		String[] expected = { "expr[string]", "null"}; //$NON-NLS-1$ //$NON-NLS-2$
		executeAndAssertResults(
				"select lookup('System.KeyColumns', 'RefKeyUID', 'KeyName', 'PK_PARTS')", expected); //$NON-NLS-1$
		
	}
}
