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

import org.junit.Before;
import org.junit.Test;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;

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

	@Test public void testModels() {

		String[] expected = {
				"Name[string]	IsPhysical[boolean]	SupportsWhereAll[boolean]	SupportsOrderBy[boolean]	SupportsJoin[boolean]	SupportsDistinct[boolean]	SupportsOuterJoin[boolean]	MaxSetSize[integer]	UID[string]	Description[string]	PrimaryMetamodelURI[string]", //$NON-NLS-1$
				"PartsSupplier	true	true	true	true	true	true	100	mmuuid:19c7f380-73d8-1edc-a81c-ecf397b10590	null	http://www.metamatrix.com/metamodels/Relational", //$NON-NLS-1$
				"System	false	true	true	true	true	true	0	mmuuid:70ffc880-29d8-1de6-8a38-9d76e1f90f2e	System	http://www.metamatrix.com/metamodels/Relational", //$NON-NLS-1$

		};
		executeAndAssertResults("select* from System.Models", expected); //$NON-NLS-1$
	}

	@Test public void testKeys() {

		String[] expected = {
				"ModelName[string]	GroupFullName[string]	Name[string]	Description[string]	NameInSource[string]	Type[string]	IsIndexed[boolean]	GroupName[string]	GroupUpperName[string]	RefKeyUID[string]	UID[string]", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.PARTS	PK_PARTS	null	null	Primary	false	PARTS	PARTS	null	mmuuid:07db4240-73ff-1edc-a81c-ecf397b10590", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SHIP_VIA	PK_SHIP_VIA	null	null	Primary	false	SHIP_VIA	SHIP_VIA	null	mmuuid:18aa3cc0-73ff-1edc-a81c-ecf397b10590", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.STATUS	PK_STATUS	null	null	Primary	false	STATUS	STATUS	null	mmuuid:25a8a740-73ff-1edc-a81c-ecf397b10590", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER	PK_SUPPLIER	null	null	Primary	false	SUPPLIER	SUPPLIER	null	mmuuid:375c8380-73ff-1edc-a81c-ecf397b10590", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	PK_SUPPLIER_PARTS	null	null	Primary	false	SUPPLIER_PARTS	SUPPLIER_PARTS	null	mmuuid:455e5440-73ff-1edc-a81c-ecf397b10590", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER	FK_SPLIER_STATS	null	FK_SPLIER_STATS	Foreign	false	SUPPLIER	SUPPLIER	mmuuid:25a8a740-73ff-1edc-a81c-ecf397b10590	mmuuid:5ac43c00-73ff-1edc-a81c-ecf397b10590", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	FK_SPLIER_PRTS_PRTS	null	FK_SPLIER_PRTS_PRTS	Foreign	false	SUPPLIER_PARTS	SUPPLIER_PARTS	mmuuid:07db4240-73ff-1edc-a81c-ecf397b10590	mmuuid:66ddc4c0-73ff-1edc-a81c-ecf397b10590", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	FK_SPLY_PRTS_SPLY	null	FK_SPLY_PRTS_SPLY	Foreign	false	SUPPLIER_PARTS	SUPPLIER_PARTS	mmuuid:375c8380-73ff-1edc-a81c-ecf397b10590	mmuuid:66ddc4c1-73ff-1edc-a81c-ecf397b10590", //$NON-NLS-1$

		};
		executeAndAssertResults("select* from System.Keys order by UID", //$NON-NLS-1$
				expected);
	}

	@Test public void testGroups() {

		String[] expected = {
				"ModelName[string]	FullName[string]	Name[string]	Type[string]	NameInSource[string]	IsPhysical[boolean]	UpperName[string]	SupportsUpdates[boolean]	UID[string]	Cardinality[integer]	Description[string]	IsSystem[boolean]	IsMaterialized[boolean]", //$NON-NLS-1$
				"System	System.Models	Models	Table	null	false	MODELS	false	mmuuid:0193bfc0-e013-1ddf-aa2e-88f814a79e93	0	null	true	false", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SHIP_VIA	SHIP_VIA	Table	SHIP_VIA	true	SHIP_VIA	true	mmuuid:0f4e9b80-73ff-1edc-a81c-ecf397b10590	4	null	false	false", //$NON-NLS-1$
				"System	System.ModelProperties	ModelProperties	Table	null	false	MODELPROPERTIES	false	mmuuid:135f7080-c370-1de7-b515-bad6cb0abb8d	0	null	true	false", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.STATUS	STATUS	Table	STATUS	true	STATUS	true	mmuuid:1f297200-73ff-1edc-a81c-ecf397b10590	3	null	false	false", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER	SUPPLIER	Table	SUPPLIER	true	SUPPLIER	true	mmuuid:2c371ec0-73ff-1edc-a81c-ecf397b10590	16	null	false	false", //$NON-NLS-1$
				"System	System.ProcedureProperties	ProcedureProperties	Table	null	false	PROCEDUREPROPERTIES	false	mmuuid:2ec88080-c5b1-1de7-b515-bad6cb0abb8d	0	null	true	false", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	SUPPLIER_PARTS	Table	SUPPLIER_PARTS	true	SUPPLIER_PARTS	true	mmuuid:3deafb00-73ff-1edc-a81c-ecf397b10590	227	null	false	false", //$NON-NLS-1$
				"System	System.ODBC.OA_TABLES	OA_TABLES	Table	null	false	OA_TABLES	false	mmuuid:54cbf440-2ca5-1f8d-a539-a73e664462a1	0	null	true	false", //$NON-NLS-1$
				"System	System.ODBC.OA_COLUMNS	OA_COLUMNS	Table	null	false	OA_COLUMNS	false	mmuuid:54cbf446-2ca5-1f8d-a539-a73e664462a1	0	null	true	false", //$NON-NLS-1$
				"System	System.ODBC.OA_PROC	OA_PROC	Table	null	false	OA_PROC	false	mmuuid:55b0d600-2ca5-1f8d-a539-a73e664462a1	0	null	true	false", //$NON-NLS-1$
				"System	System.ODBC.OA_PROCCOLUMNS	OA_PROCCOLUMNS	Table	null	false	OA_PROCCOLUMNS	false	mmuuid:56a4fa00-2ca5-1f8d-a539-a73e664462a1	0	null	true	false", //$NON-NLS-1$
				"System	System.ODBC.OA_TYPES	OA_TYPES	Table	null	false	OA_TYPES	false	mmuuid:57991e00-2ca5-1f8d-a539-a73e664462a1	0	null	true	false", //$NON-NLS-1$
				"System	System.ODBC.OA_STATISTICS	OA_STATISTICS	Table	null	false	OA_STATISTICS	false	mmuuid:57991e10-2ca5-1f8d-a539-a73e664462a1	0	null	true	false", //$NON-NLS-1$
				"System	System.ODBC.OA_FKEYS	OA_FKEYS	Table	null	false	OA_FKEYS	false	mmuuid:587dffc0-2ca5-1f8d-a539-a73e664462a1	0	null	true	false", //$NON-NLS-1$
				"System	System.DataTypeElementProperties	DataTypeElementProperties	Table	null	false	DATATYPEELEMENTPROPERTIES	false	mmuuid:64bdb6c0-c7fe-1de7-ad1c-f3ce4292824e	0	null	true	false", //$NON-NLS-1$
				"System	System.Elements	Elements	Table	null	false	ELEMENTS	false	mmuuid:6c04a1c0-0d14-1de0-be14-9d00a629c112	0	null	true	false", //$NON-NLS-1$
				"System	System.VirtualDatabases	VirtualDatabases	Table	null	false	VIRTUALDATABASES	false	mmuuid:7ce634df-2953-1de6-8a38-9d76e1f90f2e	0	null	true	false", //$NON-NLS-1$
				"System	System.KeyProperties	KeyProperties	Table	null	false	KEYPROPERTIES	false	mmuuid:8f87b840-10f8-1ff5-a438-98ce9bfae8da	0	null	true	false", //$NON-NLS-1$
				"System	System.JDBC.ReferenceKeyColumns	ReferenceKeyColumns	Table	null	false	REFERENCEKEYCOLUMNS	false	mmuuid:9d4fe980-2c99-1f8d-a539-a73e664462a1	0	null	true	false", //$NON-NLS-1$
				"System	System.DataTypeElements	DataTypeElements	Table	null	false	DATATYPEELEMENTS	false	mmuuid:b22df900-c6f1-1de7-b515-bad6cb0abb8d	0	null	true	false", //$NON-NLS-1$
				"System	System.ElementProperties	ElementProperties	Table	null	false	ELEMENTPROPERTIES	false	mmuuid:b4c43500-c514-1de7-b515-bad6cb0abb8d	0	null	true	false", //$NON-NLS-1$
				"System	System.ProcedureParams	ProcedureParams	Table	null	false	PROCEDUREPARAMS	false	mmuuid:b86f5f40-1107-1de0-8701-e0b030c0afb8	0	null	true	false", //$NON-NLS-1$
				"System	System.Groups	Groups	Table	null	false	GROUPS	false	mmuuid:c5c23382-e000-1ddf-aa2e-88f814a79e93	0	null	true	false", //$NON-NLS-1$
				"System	System.DataTypes	DataTypes	Table	null	false	DATATYPES	false	mmuuid:c5c23386-e000-1ddf-aa2e-88f814a79e93	0	null	true	false", //$NON-NLS-1$
				"System	System.DataTypeProperties	DataTypeProperties	Table	null	false	DATATYPEPROPERTIES	false	mmuuid:c68bf240-c894-1de7-ad1c-f3ce4292824e	0	null	true	false", //$NON-NLS-1$
				"System	System.Keys	Keys	Table	null	false	KEYS	false	mmuuid:d0e3fe00-0d1f-1de0-be14-9d00a629c112	0	null	true	false", //$NON-NLS-1$
				"System	System.KeyElements	KeyElements	Table	null	false	KEYELEMENTS	false	mmuuid:ee7d0140-0efb-1de0-8c1f-ee86b9bb7e7f	0	null	true	false", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.PARTS	PARTS	Table	PARTS	true	PARTS	true	mmuuid:f6276601-73fe-1edc-a81c-ecf397b10590	16	null	false	false", //$NON-NLS-1$
				"System	System.GroupProperties	GroupProperties	Table	null	false	GROUPPROPERTIES	false	mmuuid:f97f87c0-c47f-1de7-b515-bad6cb0abb8d	0	null	true	false", //$NON-NLS-1$
				"System	System.Procedures	Procedures	Table	null	false	PROCEDURES	false	mmuuid:fd2b3700-0f9d-1de0-8701-e0b030c0afb8	0	null	true	false", //$NON-NLS-1$

		};
		executeAndAssertResults("select* from System.Groups order by UID", //$NON-NLS-1$
				expected);
	}

	@Test public void testDataTypeElements() {

		String[] expected = { "DataTypeName[string]	Name[string]	Position[integer]	Scale[integer]	ElementLength[integer]	UID[string]", }; //$NON-NLS-1$
		executeAndAssertResults("select* from System.DataTypeElements", //$NON-NLS-1$
				expected);
	}

	@Test public void testDataTypes() {

		String[] expected = {
				"Name[string]	IsStandard[boolean]	IsPhysical[boolean]	TypeName[string]	JavaClass[string]	Scale[integer]	TypeLength[integer]	NullType[string]	IsSigned[boolean]	IsAutoIncremented[boolean]	IsCaseSensitive[boolean]	Precision[integer]	Radix[integer]	SearchType[string]	UID[string]	RuntimeType[string]	BaseType[string]	Description[string]", //$NON-NLS-1$
				"ENTITIES	false	false	ENTITIES	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:20360100-e742-1e20-8c26-a038c6ed7576	string	ENTITY	null", //$NON-NLS-1$
				"ENTITY	false	false	ENTITY	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:9fece300-e71a-1e20-8c26-a038c6ed7576	string	NCName	null", //$NON-NLS-1$
				"ID	false	false	ID	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:88b13dc0-e702-1e20-8c26-a038c6ed7576	string	NCName	null", //$NON-NLS-1$
				"IDREF	false	false	IDREF	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:dd33ff40-e6df-1e20-8c26-a038c6ed7576	string	NCName	null", //$NON-NLS-1$
				"IDREFS	false	false	IDREFS	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:3c99f780-e72d-1e20-8c26-a038c6ed7576	string	IDREF	null", //$NON-NLS-1$
				"NCName	false	false	NCName	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:ac00e000-e676-1e20-8c26-a038c6ed7576	string	Name	null", //$NON-NLS-1$
				"NMTOKEN	false	false	NMTOKEN	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:4ca2ae00-3a95-1e20-921b-eeee28353879	string	token	null", //$NON-NLS-1$
				"NMTOKENS	false	false	NMTOKENS	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:4b0f8500-e6a6-1e20-8c26-a038c6ed7576	string	NMTOKEN	null", //$NON-NLS-1$
				"NOTATION	false	false	NOTATION	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:3dcaf900-e8dc-1e2a-b433-fb67ea35c07e	string	anySimpleType	null", //$NON-NLS-1$
				"Name	false	false	Name	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:e66c4600-e65b-1e20-8c26-a038c6ed7576	string	token	null", //$NON-NLS-1$
				"QName	false	false	QName	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:eeb5d780-e8c3-1e2a-b433-fb67ea35c07e	string	anySimpleType	null", //$NON-NLS-1$
				"XMLLiteral	false	false	XMLLiteral	com.metamatrix.common.types.XMLType	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:43f5274e-55e1-1f87-ba1c-eea49143eb32	xml	string	null", //$NON-NLS-1$
				"anyURI	false	false	anyURI	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:6247ec80-e8a4-1e2a-b433-fb67ea35c07e	string	anySimpleType	null", //$NON-NLS-1$
				"base64Binary	false	false	base64Binary	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:b4c99380-ebc6-1e2a-9319-8eaa9b2276c7	string	anySimpleType	null", //$NON-NLS-1$
				"bigdecimal	false	false	bigdecimal	java.math.BigDecimal	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:f2249740-a078-1e26-9b08-d6079ebe1f0d	bigdecimal	decimal	null", //$NON-NLS-1$
				"biginteger	false	false	biginteger	java.math.BigInteger	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:822b9a40-a066-1e26-9b08-d6079ebe1f0d	biginteger	decimal	null", //$NON-NLS-1$
				"blob	false	false	blob	com.metamatrix.common.types.BlobType	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:5a793100-1836-1ed0-ba0f-f2334f5fbf95	blob	base64Binary	null", //$NON-NLS-1$
				"boolean	false	false	boolean	java.lang.Boolean	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:dc476100-c483-1e24-9b01-c8207cd53eb7	boolean	anySimpleType	null", //$NON-NLS-1$
				"byte	false	false	byte	java.lang.Byte	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:26dc1cc0-b9c8-1e21-b812-969c8fc8b016	byte	short	null", //$NON-NLS-1$
				"char	false	false	char	java.lang.Character	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:62472700-a064-1e26-9b08-d6079ebe1f0d	char	string	null", //$NON-NLS-1$
				"clob	false	false	clob	com.metamatrix.common.types.ClobType	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:559646c0-4941-1ece-b22b-f49159d22ad3	clob	string	null", //$NON-NLS-1$
				"date	false	false	date	java.sql.Date	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:65dcde00-c4ab-1e24-9b01-c8207cd53eb7	date	anySimpleType	null", //$NON-NLS-1$
				"dateTime	false	false	dateTime	java.sql.Timestamp	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:5c69dec0-b3ea-1e2a-9a03-beb8638ffd21	timestamp	anySimpleType	null", //$NON-NLS-1$
				"decimal	false	false	decimal	java.math.BigDecimal	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:569dfa00-c456-1e24-9b01-c8207cd53eb7	bigdecimal	anySimpleType	null", //$NON-NLS-1$
				"double	false	false	double	java.lang.Double	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:1f18b140-c4a3-1e24-9b01-c8207cd53eb7	double	anySimpleType	null", //$NON-NLS-1$
				"duration	false	false	duration	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:28d98540-b3e7-1e2a-9a03-beb8638ffd21	string	anySimpleType	null", //$NON-NLS-1$
				"float	false	false	float	java.lang.Float	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:d86b0d00-c48a-1e24-9b01-c8207cd53eb7	float	anySimpleType	null", //$NON-NLS-1$
				"gDay	false	false	gDay	java.math.BigInteger	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:860b7dc0-b3f8-1e2a-9a03-beb8638ffd21	biginteger	anySimpleType	null", //$NON-NLS-1$
				"gMonth	false	false	gMonth	java.math.BigInteger	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:187f5580-b3fb-1e2a-9a03-beb8638ffd21	biginteger	anySimpleType	null", //$NON-NLS-1$
				"gMonthDay	false	false	gMonthDay	java.sql.Timestamp	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:6e604140-b3f5-1e2a-9a03-beb8638ffd21	timestamp	anySimpleType	null", //$NON-NLS-1$
				"gYear	false	false	gYear	java.math.BigInteger	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:b02c7600-b3f2-1e2a-9a03-beb8638ffd21	biginteger	anySimpleType	null", //$NON-NLS-1$
				"gYearMonth	false	false	gYearMonth	java.sql.Timestamp	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:17d08040-b3ed-1e2a-9a03-beb8638ffd21	timestamp	anySimpleType	null", //$NON-NLS-1$
				"hexBinary	false	false	hexBinary	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:d9998500-ebba-1e2a-9319-8eaa9b2276c7	string	anySimpleType	null", //$NON-NLS-1$
				"int	false	false	int	java.lang.Integer	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:33add3c0-b98d-1e21-b812-969c8fc8b016	integer	long	null", //$NON-NLS-1$
				"integer	false	false	integer	java.math.BigInteger	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:45da3500-e78f-1e20-8c26-a038c6ed7576	biginteger	decimal	null", //$NON-NLS-1$
				"language	false	false	language	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:d4d980c0-e623-1e20-8c26-a038c6ed7576	string	token	null", //$NON-NLS-1$
				"long	false	false	long	java.lang.Long	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:8cdee840-b900-1e21-b812-969c8fc8b016	long	integer	null", //$NON-NLS-1$
				"negativeInteger	false	false	negativeInteger	java.math.BigInteger	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:86d29280-b8d3-1e21-b812-969c8fc8b016	biginteger	nonPositiveInteger	null", //$NON-NLS-1$
				"nonNegativeInteger	false	false	nonNegativeInteger	java.math.BigInteger	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:0e081200-b8a4-1e21-b812-969c8fc8b016	biginteger	integer	null", //$NON-NLS-1$
				"nonPositiveInteger	false	false	nonPositiveInteger	java.math.BigInteger	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:cbdd6e40-b9d2-1e21-8c26-a038c6ed7576	biginteger	integer	null", //$NON-NLS-1$
				"normalizedString	false	false	normalizedString	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:4df43700-3b13-1e20-921b-eeee28353879	string	string	null", //$NON-NLS-1$
				"object	false	false	object	java.lang.Object	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:051a0640-b4e8-1e26-9f33-b76fd9d5fa79	object	base64Binary	null", //$NON-NLS-1$
				"positiveInteger	false	false	positiveInteger	java.math.BigInteger	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:1cbbd380-b9ea-1e21-b812-969c8fc8b016	biginteger	nonNegativeInteger	null", //$NON-NLS-1$
				"short	false	false	short	java.lang.Short	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:5bbcf140-b9ae-1e21-b812-969c8fc8b016	short	int	null", //$NON-NLS-1$
				"string	false	false	string	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:bf6c34c0-c442-1e24-9b01-c8207cd53eb7	string	anySimpleType	null", //$NON-NLS-1$
				"time	false	false	time	java.sql.Time	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:3b892180-c4a7-1e24-9b01-c8207cd53eb7	time	anySimpleType	null", //$NON-NLS-1$
				"timestamp	false	false	timestamp	java.sql.Timestamp	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:6d9809c0-a07e-1e26-9b08-d6079ebe1f0d	timestamp	string	null", //$NON-NLS-1$
				"token	false	false	token	java.lang.String	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:3425cb80-d844-1e20-9027-be6d2c3b8b3a	string	normalizedString	null", //$NON-NLS-1$
				"unsignedByte	false	false	unsignedByte	java.lang.Short	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:cff745c0-baa2-1e21-b812-969c8fc8b016	short	unsignedShort	null", //$NON-NLS-1$
				"unsignedInt	false	false	unsignedInt	java.lang.Long	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:badcbd80-ba63-1e21-b812-969c8fc8b016	long	unsignedLong	null", //$NON-NLS-1$
				"unsignedLong	false	false	unsignedLong	java.math.BigInteger	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:54b98780-ba14-1e21-b812-969c8fc8b016	biginteger	nonNegativeInteger	null", //$NON-NLS-1$
				"unsignedShort	false	false	unsignedShort	java.lang.Integer	0	0	No Nulls	false	false	false	0	0	Searchable	mmuuid:327093c0-ba88-1e21-b812-969c8fc8b016	integer	unsignedInt	null", //$NON-NLS-1$

		};
		executeAndAssertResults("select* from System.DataTypes", expected); //$NON-NLS-1$

	}

	@Test public void testProcedureParams() {

		String[] expected = {
				"ModelName[string]	ProcedureName[string]	Name[string]	DataType[string]	Position[integer]	Type[string]	Optional[boolean]	Precision[integer]	TypeLength[integer]	Scale[integer]	Radix[integer]	NullType[string]", //$NON-NLS-1$
				"System	System.describe	Description	string	1	ResultSet	false	128	128	0	10	No Nulls", //$NON-NLS-1$
				"System	System.getVDBResourcePaths	ResourcePath	string	1	ResultSet	false	50	50	0	10	Nullable", //$NON-NLS-1$
				"System	System.getBinaryVDBResource	VdbResource	blob	1	ResultSet	false	0	0	0	10	Nullable", //$NON-NLS-1$
				"System	System.getCharacterVDBResource	VdbResource	clob	1	ResultSet	false	0	0	0	10	Nullable", //$NON-NLS-1$
				"System	System.getUpdatedCharacterVDBResource	VdbResource	clob	1	ResultSet	false	0	0	0	10	Nullable", //$NON-NLS-1$
				"System	System.describe	entity	string	1	In	false	0	0	0	10	No Nulls", //$NON-NLS-1$
				"System	System.getVDBResourcePaths	isBinary	boolean	2	ResultSet	false	1	1	0	10	Nullable", //$NON-NLS-1$
				"System	System.getBinaryVDBResource	resourcePath	string	1	In	false	50	50	0	10	No Nulls", //$NON-NLS-1$
				"System	System.getCharacterVDBResource	resourcePath	string	1	In	false	50	50	0	10	No Nulls", //$NON-NLS-1$
				"System	System.getUpdatedCharacterVDBResource	resourcePath	string	1	In	false	50	50	0	10	No Nulls", //$NON-NLS-1$
				"System	System.getUpdatedCharacterVDBResource	tokenReplacements	object	3	In	false	0	0	0	10	No Nulls", //$NON-NLS-1$
				"System	System.getUpdatedCharacterVDBResource	tokens	object	2	In	false	0	0	0	10	No Nulls", //$NON-NLS-1$

		};
		executeAndAssertResults(
				"select * from System.ProcedureParams order by Name", expected); //$NON-NLS-1$
	}

	@Test public void testProcedures() {

		String[] expected = {
				"ModelName[string]	Name[string]	NameInSource[string]	ReturnsResults[boolean]	ModelUID[string]	UID[string]	Description[string]	FullName[string]", //$NON-NLS-1$
				"System	describe	null	true	mmuuid:70ffc880-29d8-1de6-8a38-9d76e1f90f2e	mmuuid:93687100-1916-1e87-8525-f813a949866a	null	System.describe", //$NON-NLS-1$
				"System	getBinaryVDBResource	null	true	mmuuid:70ffc880-29d8-1de6-8a38-9d76e1f90f2e	mmuuid:b85dbd44-39d9-1f33-9f26-c47bba154acc	null	System.getBinaryVDBResource", //$NON-NLS-1$
				"System	getCharacterVDBResource	null	true	mmuuid:70ffc880-29d8-1de6-8a38-9d76e1f90f2e	mmuuid:b85dbd40-39d9-1f33-9f26-c47bba154acc	null	System.getCharacterVDBResource", //$NON-NLS-1$
				"System	getUpdatedCharacterVDBResource	null	true	mmuuid:70ffc880-29d8-1de6-8a38-9d76e1f90f2e	mmuuid:b9429f00-39d9-1f33-9f26-c47bba154acc	null	System.getUpdatedCharacterVDBResource", //$NON-NLS-1$
				"System	getVDBResourcePaths	null	true	mmuuid:70ffc880-29d8-1de6-8a38-9d76e1f90f2e	mmuuid:b9429f06-39d9-1f33-9f26-c47bba154acc	null	System.getVDBResourcePaths", //$NON-NLS-1$

		};
		executeAndAssertResults("select* from System.Procedures", expected); //$NON-NLS-1$
	}

	@Test public void testGroupProperties() {

		String[] expected = { "ModelName[string]	GroupFullName[string]	Name[string]	Value[string]	GroupName[string]	GroupUpperName[string]	UID[string]", }; //$NON-NLS-1$
		executeAndAssertResults("select* from System.GroupProperties", expected); //$NON-NLS-1$
	}

	@Test public void testModelProperties() {

		String[] expected = { "ModelName[string]	Name[string]	Value[string]	UID[string]", }; //$NON-NLS-1$
		executeAndAssertResults("select* from System.ModelProperties", expected); //$NON-NLS-1$
	}

	@Test public void testProcedureProperties() {

		String[] expected = { "ModelName[string]	ProcedureName[string]	Name[string]	Value[string]	UID[string]", //$NON-NLS-1$

		};
		executeAndAssertResults("select* from System.ProcedureProperties", //$NON-NLS-1$
				expected);
	}

	@Test public void testVirtualDatabase() {

		String[] expected = { "Name[string]	Version[string]	", "PartsSupplier	1", //$NON-NLS-1$ //$NON-NLS-2$

		};
		executeAndAssertResults("select* from System.VirtualDatabases", //$NON-NLS-1$
				expected);
	}

	@Test public void testKeyElements() {

		String[] expected = {
				"ModelName[string]	GroupFullName[string]	Name[string]	KeyName[string]	KeyType[string]	GroupName[string]	GroupUpperName[string]	RefKeyUID[string]	UID[string]	Position[integer]", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.PARTS	PART_ID	PK_PARTS	Primary	PARTS	PARTS	null	mmuuid:07db4240-73ff-1edc-a81c-ecf397b10590	1", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SHIP_VIA	SHIPPER_ID	PK_SHIP_VIA	Primary	SHIP_VIA	SHIP_VIA	null	mmuuid:18aa3cc0-73ff-1edc-a81c-ecf397b10590	1", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.STATUS	STATUS_ID	PK_STATUS	Primary	STATUS	STATUS	null	mmuuid:25a8a740-73ff-1edc-a81c-ecf397b10590	1", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER	SUPPLIER_ID	PK_SUPPLIER	Primary	SUPPLIER	SUPPLIER	null	mmuuid:375c8380-73ff-1edc-a81c-ecf397b10590	1", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER	SUPPLIER_STATUS	FK_SPLIER_STATS	Foreign	SUPPLIER	SUPPLIER	mmuuid:25a8a740-73ff-1edc-a81c-ecf397b10590	mmuuid:5ac43c00-73ff-1edc-a81c-ecf397b10590	1", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	PART_ID	FK_SPLIER_PRTS_PRTS	Foreign	SUPPLIER_PARTS	SUPPLIER_PARTS	mmuuid:07db4240-73ff-1edc-a81c-ecf397b10590	mmuuid:66ddc4c0-73ff-1edc-a81c-ecf397b10590	1", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	PART_ID	PK_SUPPLIER_PARTS	Primary	SUPPLIER_PARTS	SUPPLIER_PARTS	null	mmuuid:455e5440-73ff-1edc-a81c-ecf397b10590	2", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	SUPPLIER_ID	FK_SPLY_PRTS_SPLY	Foreign	SUPPLIER_PARTS	SUPPLIER_PARTS	mmuuid:375c8380-73ff-1edc-a81c-ecf397b10590	mmuuid:66ddc4c1-73ff-1edc-a81c-ecf397b10590	1", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	SUPPLIER_ID	PK_SUPPLIER_PARTS	Primary	SUPPLIER_PARTS	SUPPLIER_PARTS	null	mmuuid:455e5440-73ff-1edc-a81c-ecf397b10590	1", //$NON-NLS-1$

		};
		executeAndAssertResults(
				"select* from System.KeyElements order by GroupFullName, Name, KeyName", //$NON-NLS-1$
				expected);
	}

	@Test public void testElementProperties() {

		String[] expected = { "ModelName[string]	GroupFullName[string]	ElementName[string]	Name[string]	Value[string]	GroupName[string]	ElementUpperName[string]	GroupUpperName[string]	UID[string]", }; //$NON-NLS-1$
		executeAndAssertResults("select* from System.ElementProperties", //$NON-NLS-1$
				expected);

	}

	@Test public void testDataTypeElementProperties() {

		String[] expected = { "DataTypeName[string]	DataTypeElementName[string]	Name[string]	Value[string]	UID[string]", //$NON-NLS-1$

		};
		executeAndAssertResults(
				"select* from System.DataTypeElementProperties", expected); //$NON-NLS-1$
	}

	@Test public void testDescribe() {
		execute("exec System.describe(?)", new Object[] {"parts"}); //$NON-NLS-1$ //$NON-NLS-2$
		String[] expected = { "Description[string]", }; //$NON-NLS-1$
		assertResults(expected);
	}

	@Test public void testVDBResourcePathsProcedure() {

		String[] expected = { "ResourcePath[string]	isBinary[boolean]	", //$NON-NLS-1$
				"/parts/partsmd/PartsSupplier.xmi	false", //$NON-NLS-1$
		};
		execute("exec System.getVDBResourcePaths()",new Object[] {}); //$NON-NLS-1$
		assertResults(expected);
	}

	@Test public void testElements() {

		String[] expected = {
				"ModelName[string]	GroupName[string]	GroupFullName[string]	Name[string]	Position[integer]	NameInSource[string]	DataType[string]	Scale[integer]	ElementLength[integer]	IsLengthFixed[boolean]	SupportsSelect[boolean]	SupportsUpdates[boolean]	IsCaseSensitive[boolean]	IsSigned[boolean]	IsCurrency[boolean]	IsAutoIncremented[boolean]	NullType[string]	MinRange[string]	MaxRange[string]	SearchType[string]	Format[string]	DefaultValue[string]	JavaClass[string]	Precision[integer]	CharOctetLength[integer]	Radix[integer]	GroupUpperName[string]	UpperName[string]	UID[string]	Description[string]", //$NON-NLS-1$
				"PartsSupplier	PARTS	PartsSupplier.PARTSSUPPLIER.PARTS	PART_NAME	2	PART_NAME	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	0	255	10	PARTS	PART_NAME	mmuuid:0067e900-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"PartsSupplier	PARTS	PartsSupplier.PARTSSUPPLIER.PARTS	PART_COLOR	3	PART_COLOR	string	0	30	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	0	30	10	PARTS	PART_COLOR	mmuuid:015c0d00-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"PartsSupplier	PARTS	PartsSupplier.PARTSSUPPLIER.PARTS	PART_WEIGHT	4	PART_WEIGHT	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	0	255	10	PARTS	PART_WEIGHT	mmuuid:015c0d01-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"System	Keys	System.Keys	GroupUpperName	9	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYS	GROUPUPPERNAME	mmuuid:07268b40-c2f4-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"PartsSupplier	SHIP_VIA	PartsSupplier.PARTSSUPPLIER.SHIP_VIA	SHIPPER_ID	1	SHIPPER_ID	short	0	0	true	true	true	false	true	false	false	No Nulls	null	null	All Except Like	null	null	java.lang.Short	2	0	10	SHIP_VIA	SHIPPER_ID	mmuuid:121bc540-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"PartsSupplier	SHIP_VIA	PartsSupplier.PARTSSUPPLIER.SHIP_VIA	SHIPPER_NAME	2	SHIPPER_NAME	string	0	30	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	0	30	10	SHIP_VIA	SHIPPER_NAME	mmuuid:130fe940-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	Name	1	null	string	0	100	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	100	100	10	DATATYPES	NAME	mmuuid:15b71f00-c266-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"PartsSupplier	STATUS	PartsSupplier.PARTSSUPPLIER.STATUS	STATUS_ID	1	STATUS_ID	short	0	0	true	true	true	false	true	false	false	No Nulls	null	null	All Except Like	null	null	java.lang.Short	2	0	10	STATUS	STATUS_ID	mmuuid:201d9600-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"PartsSupplier	STATUS	PartsSupplier.PARTSSUPPLIER.STATUS	STATUS_NAME	2	STATUS_NAME	string	0	30	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	0	30	10	STATUS	STATUS_NAME	mmuuid:201d9601-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"System	Models	System.Models	Description	10	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	MODELS	DESCRIPTION	mmuuid:2268b040-3a47-1edb-8606-be949cc6da52	null", //$NON-NLS-1$
				"System	ProcedureParams	System.ProcedureParams	Precision	8	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	PROCEDUREPARAMS	PRECISION	mmuuid:2626ed80-634e-1e44-a903-c1472e78d1c5	null", //$NON-NLS-1$
				"System	ModelProperties	System.ModelProperties	ModelName	1	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	MODELPROPERTIES	MODELNAME	mmuuid:2afa8d80-c3cd-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ModelProperties	System.ModelProperties	UID	4	null	string	0	50	true	true	false	false	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	MODELPROPERTIES	UID	mmuuid:2afa8d82-c3cd-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"PartsSupplier	SUPPLIER	PartsSupplier.PARTSSUPPLIER.SUPPLIER	SUPPLIER_ID	1	SUPPLIER_ID	string	0	10	false	true	true	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	0	10	10	SUPPLIER	SUPPLIER_ID	mmuuid:2f044880-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"PartsSupplier	SUPPLIER	PartsSupplier.PARTSSUPPLIER.SUPPLIER	SUPPLIER_NAME	2	SUPPLIER_NAME	string	0	30	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	0	30	10	SUPPLIER	SUPPLIER_NAME	mmuuid:2f044881-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"PartsSupplier	SUPPLIER	PartsSupplier.PARTSSUPPLIER.SUPPLIER	SUPPLIER_STATUS	3	SUPPLIER_STATUS	short	0	0	true	true	true	false	true	false	false	Nullable	null	null	All Except Like	null	null	java.lang.Short	2	0	10	SUPPLIER	SUPPLIER_STATUS	mmuuid:2f044882-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"PartsSupplier	SUPPLIER	PartsSupplier.PARTSSUPPLIER.SUPPLIER	SUPPLIER_CITY	4	SUPPLIER_CITY	string	0	30	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	0	30	10	SUPPLIER	SUPPLIER_CITY	mmuuid:2fe92a40-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"PartsSupplier	SUPPLIER	PartsSupplier.PARTSSUPPLIER.SUPPLIER	SUPPLIER_STATE	5	SUPPLIER_STATE	string	0	2	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	0	2	10	SUPPLIER	SUPPLIER_STATE	mmuuid:2fe92a41-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"System	Procedures	System.Procedures	NameInSource	3	null	string	0	255	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	PROCEDURES	NAMEINSOURCE	mmuuid:339a9a00-35fc-1dee-8e07-e3cb3008f82b	null", //$NON-NLS-1$
				"System	DataTypeElementProperties	System.DataTypeElementProperties	DataTypeName	1	null	string	0	100	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	100	100	10	DATATYPEELEMENTPROPERTIES	DATATYPENAME	mmuuid:34af31c0-c88f-1de7-ad1c-f3ce4292824e	null", //$NON-NLS-1$
				"System	DataTypeElementProperties	System.DataTypeElementProperties	DataTypeElementName	2	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	DATATYPEELEMENTPROPERTIES	DATATYPEELEMENTNAME	mmuuid:34af31c1-c88f-1de7-ad1c-f3ce4292824e	null", //$NON-NLS-1$
				"System	DataTypeElementProperties	System.DataTypeElementProperties	Name	3	null	string	0	50	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	DATATYPEELEMENTPROPERTIES	NAME	mmuuid:34af31c2-c88f-1de7-ad1c-f3ce4292824e	null", //$NON-NLS-1$
				"System	DataTypeElementProperties	System.DataTypeElementProperties	UID	5	null	string	0	50	true	true	false	false	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	DATATYPEELEMENTPROPERTIES	UID	mmuuid:34af31c3-c88f-1de7-ad1c-f3ce4292824e	null", //$NON-NLS-1$
				"System	DataTypeElementProperties	System.DataTypeElementProperties	Value	4	null	string	0	255	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	DATATYPEELEMENTPROPERTIES	VALUE	mmuuid:35941381-c88f-1de7-ad1c-f3ce4292824e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	Description	30	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTS	DESCRIPTION	mmuuid:36fd7640-3a50-1edb-8606-be949cc6da52	null", //$NON-NLS-1$
				"System	KeyElements	System.KeyElements	Position	10	null	integer	0	10	true	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	KEYELEMENTS	POSITION	mmuuid:39d26b40-396b-1edb-8606-be949cc6da52	null", //$NON-NLS-1$
				"System	ProcedureProperties	System.ProcedureProperties	ModelName	1	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	PROCEDUREPROPERTIES	MODELNAME	mmuuid:3a5c9f42-c5f6-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ProcedureProperties	System.ProcedureProperties	ProcedureName	2	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	PROCEDUREPROPERTIES	PROCEDURENAME	mmuuid:3a5c9f43-c5f6-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ProcedureProperties	System.ProcedureProperties	Name	3	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	PROCEDUREPROPERTIES	NAME	mmuuid:3a5c9f45-c5f6-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ProcedureProperties	System.ProcedureProperties	UID	5	null	string	0	50	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	PROCEDUREPROPERTIES	UID	mmuuid:3a5c9f46-c5f6-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ProcedureProperties	System.ProcedureProperties	Value	4	null	string	0	255	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	PROCEDUREPROPERTIES	VALUE	mmuuid:3a5c9f48-c5f6-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ModelProperties	System.ModelProperties	Name	2	null	string	0	255	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	MODELPROPERTIES	NAME	mmuuid:3c881f40-c3dc-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ModelProperties	System.ModelProperties	Value	3	null	string	0	255	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	MODELPROPERTIES	VALUE	mmuuid:3c881f41-c3dc-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"PartsSupplier	SUPPLIER_PARTS	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	SUPPLIER_ID	1	SUPPLIER_ID	string	0	10	false	true	true	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	0	10	10	SUPPLIER_PARTS	SUPPLIER_ID	mmuuid:3ecfdcc0-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"PartsSupplier	SUPPLIER_PARTS	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	PART_ID	2	PART_ID	string	0	4	true	true	true	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	0	4	10	SUPPLIER_PARTS	PART_ID	mmuuid:3fc400c0-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"PartsSupplier	SUPPLIER_PARTS	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	QUANTITY	3	QUANTITY	short	0	0	true	true	true	false	true	false	false	Nullable	null	null	All Except Like	null	null	java.lang.Short	3	0	10	SUPPLIER_PARTS	QUANTITY	mmuuid:3fc400c1-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"PartsSupplier	SUPPLIER_PARTS	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	SHIPPER_ID	4	SHIPPER_ID	short	0	0	true	true	true	false	true	false	false	Nullable	null	null	All Except Like	null	null	java.lang.Short	2	0	10	SUPPLIER_PARTS	SHIPPER_ID	mmuuid:3fc400c2-73ff-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"System	Models	System.Models	UID	9	null	string	0	50	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	MODELS	UID	mmuuid:4610fe00-b6d6-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypeElements	System.DataTypeElements	DataTypeName	1	null	string	0	100	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	100	100	10	DATATYPEELEMENTS	DATATYPENAME	mmuuid:49bf3600-c74a-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	DataTypeElements	System.DataTypeElements	Name	2	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	DATATYPEELEMENTS	NAME	mmuuid:49bf3601-c74a-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	DataTypeElements	System.DataTypeElements	Position	3	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	DATATYPEELEMENTS	POSITION	mmuuid:49bf3602-c74a-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	DataTypeElements	System.DataTypeElements	UID	6	null	string	0	50	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	DATATYPEELEMENTS	UID	mmuuid:49bf3604-c74a-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	DataTypeElements	System.DataTypeElements	Scale	4	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	DATATYPEELEMENTS	SCALE	mmuuid:49bf3605-c74a-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	DataTypeElements	System.DataTypeElements	ElementLength	5	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	DATATYPEELEMENTS	ELEMENTLENGTH	mmuuid:49bf3606-c74a-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	TypeLength	7	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	(0)	java.lang.Integer	10	10	10	DATATYPES	TYPELENGTH	mmuuid:505f1740-b823-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Models	System.Models	Name	1	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	MODELS	NAME	mmuuid:51706d40-b6d0-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Groups	System.Groups	SupportsUpdates	8	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	GROUPS	SUPPORTSUPDATES	mmuuid:533fa200-348c-1e06-9501-aee4d16940ab	null", //$NON-NLS-1$
				"System	OA_TABLES	System.ODBC.OA_TABLES	TABLE_OWNER	2	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_TABLES	TABLE_OWNER	mmuuid:54cbf441-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TABLES	System.ODBC.OA_TABLES	REMARKS	5	null	string	0	254	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	254	254	10	OA_TABLES	REMARKS	mmuuid:54cbf442-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TABLES	System.ODBC.OA_TABLES	TABLE_TYPE	4	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_TABLES	TABLE_TYPE	mmuuid:54cbf443-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TABLES	System.ODBC.OA_TABLES	TABLE_NAME	3	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_TABLES	TABLE_NAME	mmuuid:54cbf444-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TABLES	System.ODBC.OA_TABLES	TABLE_QUALIFIER	1	null	string	0	128	false	true	false	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_TABLES	TABLE_QUALIFIER	mmuuid:54cbf445-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	TABLE_QUALIFIER	1	null	string	0	128	false	true	false	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_COLUMNS	TABLE_QUALIFIER	mmuuid:54cbf447-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	OA_NULLABLE	11	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_COLUMNS	OA_NULLABLE	mmuuid:54cbf448-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	OA_SCALE	9	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	OA_COLUMNS	OA_SCALE	mmuuid:54cbf449-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	TABLE_NAME	3	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_COLUMNS	TABLE_NAME	mmuuid:54cbf44a-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	REMARKS	15	null	string	0	254	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	254	254	10	OA_COLUMNS	REMARKS	mmuuid:54cbf44b-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	OA_COLUMNTYPE	14	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_COLUMNS	OA_COLUMNTYPE	mmuuid:54cbf44c-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	OA_PRECISION	8	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	OA_COLUMNS	OA_PRECISION	mmuuid:54cbf44d-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	OA_RADIX	10	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	OA_COLUMNS	OA_RADIX	mmuuid:54cbf44e-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	OA_LENGTH	7	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	OA_COLUMNS	OA_LENGTH	mmuuid:54cbf44f-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	COLUMN_NAME	4	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_COLUMNS	COLUMN_NAME	mmuuid:54cbf450-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	OA_SCOPE	12	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_COLUMNS	OA_SCOPE	mmuuid:54cbf451-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	DATA_TYPE	5	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_COLUMNS	DATA_TYPE	mmuuid:54cbf452-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	TYPE_NAME	6	null	string	0	100	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	100	100	10	OA_COLUMNS	TYPE_NAME	mmuuid:54cbf453-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	PSEUDO_COLUMN	13	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_COLUMNS	PSEUDO_COLUMN	mmuuid:54cbf454-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_COLUMNS	System.ODBC.OA_COLUMNS	TABLE_OWNER	2	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_COLUMNS	TABLE_OWNER	mmuuid:54cbf455-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ElementProperties	System.ElementProperties	ModelName	1	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTPROPERTIES	MODELNAME	mmuuid:54d5e2c2-c59f-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ElementProperties	System.ElementProperties	GroupName	6	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTPROPERTIES	GROUPNAME	mmuuid:54d5e2c3-c59f-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ElementProperties	System.ElementProperties	GroupFullName	2	null	string	0	2048	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	2048	2048	10	ELEMENTPROPERTIES	GROUPFULLNAME	mmuuid:54d5e2c4-c59f-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ElementProperties	System.ElementProperties	ElementName	3	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTPROPERTIES	ELEMENTNAME	mmuuid:54d5e2c5-c59f-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ElementProperties	System.ElementProperties	Name	4	null	string	0	255	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTPROPERTIES	NAME	mmuuid:54d5e2c6-c59f-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ElementProperties	System.ElementProperties	UID	9	null	string	0	50	true	true	false	false	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	ELEMENTPROPERTIES	UID	mmuuid:54d5e2c7-c59f-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ElementProperties	System.ElementProperties	Value	5	null	string	0	255	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTPROPERTIES	VALUE	mmuuid:54d5e2c9-c59f-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	OA_PROC	System.ODBC.OA_PROC	REMARKS	7	null	string	0	254	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	254	254	10	OA_PROC	REMARKS	mmuuid:55b0d601-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROC	System.ODBC.OA_PROC	OA_OWNER	2	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_PROC	OA_OWNER	mmuuid:55b0d602-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROC	System.ODBC.OA_PROC	OA_QUALIFIER	1	null	string	0	128	false	true	false	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_PROC	OA_QUALIFIER	mmuuid:55b0d603-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROC	System.ODBC.OA_PROC	NUM_OUTPUT_PARAMS	5	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	OA_PROC	NUM_OUTPUT_PARAMS	mmuuid:55b0d604-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROC	System.ODBC.OA_PROC	NUM_INPUT_PARAMS	4	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	OA_PROC	NUM_INPUT_PARAMS	mmuuid:55b0d605-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROC	System.ODBC.OA_PROC	OA_NAME	3	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_PROC	OA_NAME	mmuuid:55b0d606-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROC	System.ODBC.OA_PROC	PROCEDURE_TYPE	8	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_PROC	PROCEDURE_TYPE	mmuuid:55b0d607-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROC	System.ODBC.OA_PROC	NUM_RESULT_SETS	6	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	OA_PROC	NUM_RESULT_SETS	mmuuid:55b0d608-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ElementProperties	System.ElementProperties	GroupUpperName	8	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTPROPERTIES	GROUPUPPERNAME	mmuuid:55bac480-c59f-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	ElementProperties	System.ElementProperties	ElementUpperName	7	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTPROPERTIES	ELEMENTUPPERNAME	mmuuid:55bac481-c59f-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	OA_LENGTH	9	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	OA_PROCCOLUMNS	OA_LENGTH	mmuuid:56a4fa01-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	OA_PRECISION	8	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	OA_PROCCOLUMNS	OA_PRECISION	mmuuid:56a4fa02-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	COLUMN_NAME	4	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_PROCCOLUMNS	COLUMN_NAME	mmuuid:56a4fa03-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	REMARKS	14	null	string	0	254	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	254	254	10	OA_PROCCOLUMNS	REMARKS	mmuuid:56a4fa04-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	OA_NAME	3	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_PROCCOLUMNS	OA_NAME	mmuuid:56a4fa05-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	OA_RADIX	10	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	OA_PROCCOLUMNS	OA_RADIX	mmuuid:56a4fa06-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	DATA_TYPE	6	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_PROCCOLUMNS	DATA_TYPE	mmuuid:56a4fa07-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	OA_SCALE	11	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	OA_PROCCOLUMNS	OA_SCALE	mmuuid:56a4fa08-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	OA_SCOPE	13	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_PROCCOLUMNS	OA_SCOPE	mmuuid:56a4fa09-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	TYPE_NAME	7	null	string	0	100	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	100	100	10	OA_PROCCOLUMNS	TYPE_NAME	mmuuid:56a4fa0a-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	OA_QUALIFIER	1	null	string	0	128	false	true	false	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_PROCCOLUMNS	OA_QUALIFIER	mmuuid:56a4fa0b-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	OA_OWNER	2	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_PROCCOLUMNS	OA_OWNER	mmuuid:56a4fa0c-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	OA_NULLABLE	12	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_PROCCOLUMNS	OA_NULLABLE	mmuuid:56a4fa0d-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_PROCCOLUMNS	System.ODBC.OA_PROCCOLUMNS	OA_COLUMNTYPE	5	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_PROCCOLUMNS	OA_COLUMNTYPE	mmuuid:56a4fa0e-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	CREATE_PARAMS	6	null	string	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	10	10	10	OA_TYPES	CREATE_PARAMS	mmuuid:57991e01-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	LITERAL_PREFIX	4	null	string	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	10	10	10	OA_TYPES	LITERAL_PREFIX	mmuuid:57991e02-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	AUTO_INCREMENT	12	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_TYPES	AUTO_INCREMENT	mmuuid:57991e03-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	OA_MONEY	11	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_TYPES	OA_MONEY	mmuuid:57991e04-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	MINIMUM_SCALE	13	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_TYPES	MINIMUM_SCALE	mmuuid:57991e05-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	OA_PRECISION	3	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	OA_TYPES	OA_PRECISION	mmuuid:57991e06-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	DATA_TYPE	2	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_TYPES	DATA_TYPE	mmuuid:57991e07-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	MAXIMUM_SCALE	14	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_TYPES	MAXIMUM_SCALE	mmuuid:57991e08-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	CASE_SENSITIVE	8	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_TYPES	CASE_SENSITIVE	mmuuid:57991e09-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	LOCAL_TYPE_NAME	15	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_TYPES	LOCAL_TYPE_NAME	mmuuid:57991e0a-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	LITERAL_SUFFIX	5	null	string	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	10	10	10	OA_TYPES	LITERAL_SUFFIX	mmuuid:57991e0b-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	UNSIGNED_ATTRIB	10	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_TYPES	UNSIGNED_ATTRIB	mmuuid:57991e0c-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	TYPE_NAME	1	null	string	0	100	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	100	100	10	OA_TYPES	TYPE_NAME	mmuuid:57991e0d-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	OA_NULLABLE	7	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_TYPES	OA_NULLABLE	mmuuid:57991e0e-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_TYPES	System.ODBC.OA_TYPES	OA_SEARCHABLE	9	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_TYPES	OA_SEARCHABLE	mmuuid:57991e0f-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	NON_UNIQUE	4	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_STATISTICS	NON_UNIQUE	mmuuid:57991e11-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	OA_PAGES	12	null	string	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	10	10	10	OA_STATISTICS	OA_PAGES	mmuuid:57991e12-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	INDEX_NAME	6	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_STATISTICS	INDEX_NAME	mmuuid:57991e13-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	SEQ_IN_INDEX	8	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_STATISTICS	SEQ_IN_INDEX	mmuuid:57991e14-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	OA_COLLATION	10	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_STATISTICS	OA_COLLATION	mmuuid:57991e15-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	OA_CARDINALITY	11	null	string	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	10	10	10	OA_STATISTICS	OA_CARDINALITY	mmuuid:57991e16-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	TABLE_OWNER	2	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_STATISTICS	TABLE_OWNER	mmuuid:57991e17-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	COLUMN_NAME	9	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_STATISTICS	COLUMN_NAME	mmuuid:57991e18-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	TABLE_NAME	3	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_STATISTICS	TABLE_NAME	mmuuid:57991e19-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	OA_TYPE	7	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_STATISTICS	OA_TYPE	mmuuid:57991e1a-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	INDEX_QUALIFIER	5	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_STATISTICS	INDEX_QUALIFIER	mmuuid:57991e1b-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	TABLE_QUALIFIER	1	null	string	0	128	false	true	false	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_STATISTICS	TABLE_QUALIFIER	mmuuid:57991e1c-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_STATISTICS	System.ODBC.OA_STATISTICS	FILTER_CONDITIONS	13	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_STATISTICS	FILTER_CONDITIONS	mmuuid:57991e1d-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	KeyElements	System.KeyElements	ModelName	1	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYELEMENTS	MODELNAME	mmuuid:57c33242-ba68-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	KeyElements	System.KeyElements	GroupName	6	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYELEMENTS	GROUPNAME	mmuuid:57c33243-ba68-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	KeyElements	System.KeyElements	GroupFullName	2	null	string	0	2048	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	2048	2048	10	KEYELEMENTS	GROUPFULLNAME	mmuuid:57c33244-ba68-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	KEY_SEQ	9	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_FKEYS	KEY_SEQ	mmuuid:587dffc1-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	PK_NAME	13	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_FKEYS	PK_NAME	mmuuid:587dffc2-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	FKTABLE_QUALIFIER	5	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_FKEYS	FKTABLE_QUALIFIER	mmuuid:587dffc3-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	FKTABLE_NAME	7	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_FKEYS	FKTABLE_NAME	mmuuid:587dffc4-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	FKTABLE_OWNER	6	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_FKEYS	FKTABLE_OWNER	mmuuid:587dffc5-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	PKCOLUMN_NAME	4	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_FKEYS	PKCOLUMN_NAME	mmuuid:587dffc6-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	FK_NAME	12	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_FKEYS	FK_NAME	mmuuid:587dffc7-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	PKTABLE_OWNER	2	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_FKEYS	PKTABLE_OWNER	mmuuid:587dffc8-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	UPDATE_RULE	10	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_FKEYS	UPDATE_RULE	mmuuid:587dffc9-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	DELETE_RULE	11	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	OA_FKEYS	DELETE_RULE	mmuuid:587dffca-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	FKCOLUMN_NAME	8	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_FKEYS	FKCOLUMN_NAME	mmuuid:587dffcb-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	PKTABLE_NAME	3	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_FKEYS	PKTABLE_NAME	mmuuid:587dffcc-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	OA_FKEYS	System.ODBC.OA_FKEYS	PKTABLE_QUALIFIER	1	null	string	0	128	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	128	128	10	OA_FKEYS	PKTABLE_QUALIFIER	mmuuid:587dffcd-2ca5-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	KeyElements	System.KeyElements	KeyName	4	null	string	0	255	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYELEMENTS	KEYNAME	mmuuid:58a81400-ba68-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	KeyElements	System.KeyElements	RefKeyUID	8	null	string	0	50	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	50	50	10	KEYELEMENTS	REFKEYUID	mmuuid:58a81402-ba68-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	KeyElements	System.KeyElements	UID	9	null	string	0	50	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	KEYELEMENTS	UID	mmuuid:58a81403-ba68-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	KeyElements	System.KeyElements	GroupUpperName	7	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYELEMENTS	GROUPUPPERNAME	mmuuid:58a81404-ba68-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	ProcedureParams	System.ProcedureParams	TypeLength	9	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	(0)	java.lang.Integer	10	10	10	PROCEDUREPARAMS	TYPELENGTH	mmuuid:5e0fac00-634e-1e44-a903-c1472e78d1c5	null", //$NON-NLS-1$
				"System	Procedures	System.Procedures	Description	7	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	225	255	10	PROCEDURES	DESCRIPTION	mmuuid:61164b00-3ab4-1edb-8606-be949cc6da52	null", //$NON-NLS-1$
				"System	DataTypeProperties	System.DataTypeProperties	DataType	1	null	string	0	100	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	100	100	10	DATATYPEPROPERTIES	DATATYPE	mmuuid:6609d102-c90a-1de7-ad1c-f3ce4292824e	null", //$NON-NLS-1$
				"System	DataTypeProperties	System.DataTypeProperties	Name	2	null	string	0	50	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	DATATYPEPROPERTIES	NAME	mmuuid:6609d103-c90a-1de7-ad1c-f3ce4292824e	null", //$NON-NLS-1$
				"System	DataTypeProperties	System.DataTypeProperties	Value	3	null	string	0	255	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	DATATYPEPROPERTIES	VALUE	mmuuid:6609d106-c90a-1de7-ad1c-f3ce4292824e	null", //$NON-NLS-1$
				"System	Models	System.Models	SupportsWhereAll	3	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	MODELS	SUPPORTSWHEREALL	mmuuid:6b529981-b67c-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Models	System.Models	SupportsOrderBy	4	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	MODELS	SUPPORTSORDERBY	mmuuid:6c377b40-b67c-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Models	System.Models	SupportsJoin	5	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	MODELS	SUPPORTSJOIN	mmuuid:6c377b42-b67c-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Models	System.Models	SupportsDistinct	6	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	MODELS	SUPPORTSDISTINCT	mmuuid:6c377b43-b67c-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Models	System.Models	SupportsOuterJoin	7	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	MODELS	SUPPORTSOUTERJOIN	mmuuid:6c377b44-b67c-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Procedures	System.Procedures	ModelName	1	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	PROCEDURES	MODELNAME	mmuuid:7a19a9c2-b863-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Procedures	System.Procedures	Name	2	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	PROCEDURES	NAME	mmuuid:7a19a9c3-b863-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Procedures	System.Procedures	ModelUID	5	null	string	0	50	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	PROCEDURES	MODELUID	mmuuid:7a19a9c6-b863-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Procedures	System.Procedures	ReturnsResults	4	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	PROCEDURES	RETURNSRESULTS	mmuuid:7afe8b81-b863-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Procedures	System.Procedures	UID	6	null	string	0	50	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	PROCEDURES	UID	mmuuid:7afe8b83-b863-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	ModelName	1	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTS	MODELNAME	mmuuid:7b0da102-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	GroupName	2	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTS	GROUPNAME	mmuuid:7b0da103-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	GroupFullName	3	null	string	0	2048	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	2048	2048	10	ELEMENTS	GROUPFULLNAME	mmuuid:7b0da104-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	Name	4	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTS	NAME	mmuuid:7b0da105-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	Position	5	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	ELEMENTS	POSITION	mmuuid:7b0da106-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	DataType	7	null	string	0	100	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	100	100	10	ELEMENTS	DATATYPE	mmuuid:7bf282c3-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	Scale	8	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	ELEMENTS	SCALE	mmuuid:7bf282c4-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	ElementLength	9	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	ELEMENTS	ELEMENTLENGTH	mmuuid:7bf282c5-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	IsLengthFixed	10	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	ELEMENTS	ISLENGTHFIXED	mmuuid:7bf282c7-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	SupportsSelect	11	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	ELEMENTS	SUPPORTSSELECT	mmuuid:7bf282c8-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	SupportsUpdates	12	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	ELEMENTS	SUPPORTSUPDATES	mmuuid:7ce6a6c0-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	IsCaseSensitive	13	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	ELEMENTS	ISCASESENSITIVE	mmuuid:7ce6a6c1-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	IsSigned	14	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	ELEMENTS	ISSIGNED	mmuuid:7ce6a6c2-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	IsCurrency	15	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	ELEMENTS	ISCURRENCY	mmuuid:7ce6a6c3-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	IsAutoIncremented	16	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	ELEMENTS	ISAUTOINCREMENTED	mmuuid:7ce6a6c4-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	NullType	17	null	string	0	20	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	20	20	10	ELEMENTS	NULLTYPE	mmuuid:7ce6a6c5-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	MinRange	18	null	string	0	50	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	50	50	10	ELEMENTS	MINRANGE	mmuuid:7ce6a6c6-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	MaxRange	19	null	string	0	50	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	50	50	10	ELEMENTS	MAXRANGE	mmuuid:7ce6a6c7-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	SearchType	20	null	string	0	20	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	20	20	10	ELEMENTS	SEARCHTYPE	mmuuid:7ce6a6c8-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	Format	21	null	string	0	255	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTS	FORMAT	mmuuid:7ce6a6c9-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	DefaultValue	22	null	string	0	255	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTS	DEFAULTVALUE	mmuuid:7ddacac1-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	JavaClass	23	null	string	0	500	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	500	500	10	ELEMENTS	JAVACLASS	mmuuid:7ddacac2-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	UID	29	null	string	0	50	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	ELEMENTS	UID	mmuuid:7ddacac3-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	Precision	24	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	ELEMENTS	PRECISION	mmuuid:7ddacac5-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Elements	System.Elements	Radix	26	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	ELEMENTS	RADIX	mmuuid:7ddacac7-b9b3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	VirtualDatabases	System.VirtualDatabases	Name	1	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	VIRTUALDATABASES	NAME	mmuuid:8cfee540-c1d3-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	VirtualDatabases	System.VirtualDatabases	Version	2	null	string	0	50	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	VIRTUALDATABASES	VERSION	mmuuid:8cfee541-c1d3-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	Elements	System.Elements	NameInSource	6	null	string	0	255	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTS	NAMEINSOURCE	mmuuid:8f854500-3416-1dee-8e07-e3cb3008f82b	null", //$NON-NLS-1$
				"System	ProcedureParams	System.ProcedureParams	Type	6	null	string	0	100	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	100	100	10	PROCEDUREPARAMS	TYPE	mmuuid:91fce1c0-6663-1de8-a732-e7f41500bc3a	null", //$NON-NLS-1$
				"System	ProcedureParams	System.ProcedureParams	Scale	10	null	integer	0	10	true	true	true	false	false	false	false	No Nulls	null	null	Searchable	null	(0)	java.lang.Integer	10	10	10	PROCEDUREPARAMS	SCALE	mmuuid:95044680-634e-1e44-a903-c1472e78d1c5	null", //$NON-NLS-1$
				"System	ProcedureParams	System.ProcedureParams	ModelName	1	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	PROCEDUREPARAMS	MODELNAME	mmuuid:959eca00-b8f3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	ProcedureParams	System.ProcedureParams	ProcedureName	2	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	PROCEDUREPARAMS	PROCEDURENAME	mmuuid:959eca01-b8f3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	ProcedureParams	System.ProcedureParams	Name	3	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	PROCEDUREPARAMS	NAME	mmuuid:959eca03-b8f3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	ProcedureParams	System.ProcedureParams	DataType	4	null	string	0	25	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	25	25	10	PROCEDUREPARAMS	DATATYPE	mmuuid:959eca04-b8f3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	ProcedureParams	System.ProcedureParams	Position	5	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	PROCEDUREPARAMS	POSITION	mmuuid:959eca06-b8f3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	ProcedureParams	System.ProcedureParams	Optional	7	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	PROCEDUREPARAMS	OPTIONAL	mmuuid:959eca08-b8f3-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Keys	System.Keys	NameInSource	5	null	string	0	255	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYS	NAMEINSOURCE	mmuuid:9a749d40-3424-1dee-8e07-e3cb3008f82b	null", //$NON-NLS-1$
				"System	KeyProperties	System.KeyProperties	ModelName	1	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYPROPERTIES	MODELNAME	mmuuid:9ba7aa01-1142-1ff5-a438-98ce9bfae8da	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	FKTABLE_CAT	5	null	string	0	1	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	1	1	10	REFERENCEKEYCOLUMNS	FKTABLE_CAT	mmuuid:9d4fe981-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	DELETE_RULE	11	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	REFERENCEKEYCOLUMNS	DELETE_RULE	mmuuid:9d4fe982-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	FKTABLE_SCHEM	6	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	REFERENCEKEYCOLUMNS	FKTABLE_SCHEM	mmuuid:9d4fe983-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	PK_NAME	13	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	REFERENCEKEYCOLUMNS	PK_NAME	mmuuid:9d4fe984-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	FKCOLUMN_NAME	8	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	REFERENCEKEYCOLUMNS	FKCOLUMN_NAME	mmuuid:9d4fe985-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	PKTABLE_NAME	3	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	REFERENCEKEYCOLUMNS	PKTABLE_NAME	mmuuid:9d4fe986-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	FK_NAME	12	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	REFERENCEKEYCOLUMNS	FK_NAME	mmuuid:9d4fe987-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	KEY_SEQ	9	null	short	0	5	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Short	5	5	10	REFERENCEKEYCOLUMNS	KEY_SEQ	mmuuid:9d4fe988-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	PKTABLE_CAT	1	null	string	0	1	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	1	1	10	REFERENCEKEYCOLUMNS	PKTABLE_CAT	mmuuid:9d4fe989-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	UPDATE_RULE	10	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	REFERENCEKEYCOLUMNS	UPDATE_RULE	mmuuid:9d4fe98a-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	FKTABLE_NAME	7	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	REFERENCEKEYCOLUMNS	FKTABLE_NAME	mmuuid:9d4fe98b-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	DEFERRABILITY	14	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	REFERENCEKEYCOLUMNS	DEFERRABILITY	mmuuid:9d4fe98c-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	PKCOLUMN_NAME	4	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	REFERENCEKEYCOLUMNS	PKCOLUMN_NAME	mmuuid:9d4fe98d-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	ReferenceKeyColumns	System.JDBC.ReferenceKeyColumns	PKTABLE_SCHEM	2	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	REFERENCEKEYCOLUMNS	PKTABLE_SCHEM	mmuuid:9d4fe98e-2c99-1f8d-a539-a73e664462a1	null", //$NON-NLS-1$
				"System	KeyProperties	System.KeyProperties	GroupFullName	2	null	string	0	2048	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	2048	2048	10	KEYPROPERTIES	GROUPFULLNAME	mmuuid:9d80afc0-1142-1ff5-a438-98ce9bfae8da	null", //$NON-NLS-1$
				"System	Keys	System.Keys	Type	6	null	string	0	20	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	20	20	10	KEYS	TYPE	mmuuid:9e0dce80-ce82-1de7-8c2d-d908d66ab0ba	null", //$NON-NLS-1$
				"System	Groups	System.Groups	IsMaterialized	13	null	boolean	0	0	false	true	true	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	0	0	10	GROUPS	ISMATERIALIZED	mmuuid:9e5c3980-04e6-101e-861a-a893857ac5a5	null", //$NON-NLS-1$
				"System	KeyProperties	System.KeyProperties	KeyName	3	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	0	255	10	KEYPROPERTIES	KEYNAME	mmuuid:9e74d3c0-1142-1ff5-a438-98ce9bfae8da	null", //$NON-NLS-1$
				"System	KeyProperties	System.KeyProperties	Name	4	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	0	255	10	KEYPROPERTIES	NAME	mmuuid:9e74d3c5-1142-1ff5-a438-98ce9bfae8da	null", //$NON-NLS-1$
				"System	KeyProperties	System.KeyProperties	Value	5	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	0	255	10	KEYPROPERTIES	VALUE	mmuuid:9e74d3ca-1142-1ff5-a438-98ce9bfae8da	null", //$NON-NLS-1$
				"System	KeyProperties	System.KeyProperties	GroupName	6	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYPROPERTIES	GROUPNAME	mmuuid:9e74d3cf-1142-1ff5-a438-98ce9bfae8da	null", //$NON-NLS-1$
				"System	KeyProperties	System.KeyProperties	GroupUpperName	7	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYPROPERTIES	GROUPUPPERNAME	mmuuid:9e74d3d4-1142-1ff5-a438-98ce9bfae8da	null", //$NON-NLS-1$
				"System	KeyProperties	System.KeyProperties	UID	8	null	string	0	50	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	KEYPROPERTIES	UID	mmuuid:9f68f7c0-1142-1ff5-a438-98ce9bfae8da	null", //$NON-NLS-1$
				"System	Models	System.Models	PrimaryMetamodelURI	11	null	string	0	255	false	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	MODELS	PRIMARYMETAMODELURI	mmuuid:a5682040-ea71-1ee5-b836-ce1850f9b1e5	null", //$NON-NLS-1$
				"System	Procedures	System.Procedures	FullName	8	null	string	0	2048	false	true	true	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	2048	2048	10	PROCEDURES	FULLNAME	mmuuid:af58c3c0-23d0-1f11-bb31-8e2670f95fb1	null", //$NON-NLS-1$
				"System	Groups	System.Groups	Description	11	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	GROUPS	DESCRIPTION	mmuuid:afce0780-3a4e-1edb-8606-be949cc6da52	null", //$NON-NLS-1$
				"System	Groups	System.Groups	UID	9	null	string	0	50	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	GROUPS	UID	mmuuid:b1e4e240-ec67-1de9-9115-ad36abcfb081	null", //$NON-NLS-1$
				"System	Groups	System.Groups	ModelName	1	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	GROUPS	MODELNAME	mmuuid:b1e4e243-ec67-1de9-9115-ad36abcfb081	null", //$NON-NLS-1$
				"System	Groups	System.Groups	FullName	2	null	string	0	2048	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	2048	2048	10	GROUPS	FULLNAME	mmuuid:b1e4e244-ec67-1de9-9115-ad36abcfb081	null", //$NON-NLS-1$
				"System	Groups	System.Groups	Name	3	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	GROUPS	NAME	mmuuid:b1e4e245-ec67-1de9-9115-ad36abcfb081	null", //$NON-NLS-1$
				"System	Groups	System.Groups	IsPhysical	6	null	boolean	0	1	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	GROUPS	ISPHYSICAL	mmuuid:b1e4e248-ec67-1de9-9115-ad36abcfb081	null", //$NON-NLS-1$
				"System	Groups	System.Groups	Cardinality	10	null	integer	0	10	false	true	false	true	true	false	false	No Nulls	null	null	All Except Like	null	null	java.lang.Integer	10	10	10	GROUPS	CARDINALITY	mmuuid:b2d85ec0-ebd5-1eda-b235-cf50afc035e1	null", //$NON-NLS-1$
				"System	Groups	System.Groups	UpperName	7	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	GROUPS	UPPERNAME	mmuuid:b2d90641-ec67-1de9-9115-ad36abcfb081	null", //$NON-NLS-1$
				"System	Groups	System.Groups	Type	4	null	string	0	20	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	20	20	10	GROUPS	TYPE	mmuuid:b2d90642-ec67-1de9-9115-ad36abcfb081	null", //$NON-NLS-1$
				"System	Elements	System.Elements	CharOctetLength	25	null	integer	0	10	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	ELEMENTS	CHAROCTETLENGTH	mmuuid:c0b5e1c0-cdd8-1de7-8c2d-d908d66ab0ba	null", //$NON-NLS-1$
				"System	Groups	System.Groups	NameInSource	5	null	string	0	255	true	true	false	true	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	GROUPS	NAMEINSOURCE	mmuuid:c143df00-3409-1dee-8e07-e3cb3008f82b	null", //$NON-NLS-1$
				"System	ProcedureParams	System.ProcedureParams	Radix	11	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	PROCEDUREPARAMS	RADIX	mmuuid:c65e8d80-634e-1e44-a903-c1472e78d1c5	null", //$NON-NLS-1$
				"System	GroupProperties	System.GroupProperties	ModelName	1	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	GROUPPROPERTIES	MODELNAME	mmuuid:c8e43042-c509-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	GroupProperties	System.GroupProperties	GroupName	5	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	GROUPPROPERTIES	GROUPNAME	mmuuid:c8e43043-c509-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	GroupProperties	System.GroupProperties	GroupFullName	2	null	string	0	2048	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	2048	2048	10	GROUPPROPERTIES	GROUPFULLNAME	mmuuid:c8e43044-c509-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	GroupProperties	System.GroupProperties	Name	3	null	string	0	255	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	GROUPPROPERTIES	NAME	mmuuid:c8e43045-c509-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	GroupProperties	System.GroupProperties	UID	7	null	string	0	50	true	true	false	false	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	GROUPPROPERTIES	UID	mmuuid:c8e43046-c509-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	GroupProperties	System.GroupProperties	Value	4	null	string	0	255	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	GROUPPROPERTIES	VALUE	mmuuid:c8e43048-c509-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	GroupProperties	System.GroupProperties	GroupUpperName	6	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	GROUPPROPERTIES	GROUPUPPERNAME	mmuuid:c8e43049-c509-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	Models	System.Models	IsPhysical	2	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	MODELS	ISPHYSICAL	mmuuid:d5a73440-b6b7-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Models	System.Models	MaxSetSize	8	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	MODELS	MAXSETSIZE	mmuuid:d5a73442-b6b7-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	KeyElements	System.KeyElements	KeyType	5	null	string	0	20	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	20	20	10	KEYELEMENTS	KEYTYPE	mmuuid:db288f80-ce87-1de7-8c2d-d908d66ab0ba	null", //$NON-NLS-1$
				"System	DataTypeProperties	System.DataTypeProperties	UID	4	null	string	0	50	true	true	false	false	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	DATATYPEPROPERTIES	UID	mmuuid:dba25280-a5e0-1e42-ba2a-a0f062ab71df	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	RuntimeType	16	null	string	0	64	true	true	false	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	64	64	10	DATATYPES	RUNTIMETYPE	mmuuid:de321500-e584-1e20-b806-d8b2a0a91d66	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	IsStandard	2	null	boolean	0	1	true	true	false	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	DATATYPES	ISSTANDARD	mmuuid:e21059c5-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	IsPhysical	3	null	boolean	0	1	true	true	false	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	DATATYPES	ISPHYSICAL	mmuuid:e2f53b80-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	TypeName	4	null	string	0	100	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	100	100	10	DATATYPES	TYPENAME	mmuuid:e2f53b81-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	Scale	6	null	integer	0	10	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	(0)	java.lang.Integer	10	10	10	DATATYPES	SCALE	mmuuid:e2f53b82-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	NullType	8	null	string	0	20	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	20	20	10	DATATYPES	NULLTYPE	mmuuid:e2f53b84-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	IsSigned	9	null	boolean	0	1	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	('0')	java.lang.Boolean	1	1	10	DATATYPES	ISSIGNED	mmuuid:e2f53b85-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	IsAutoIncremented	10	null	boolean	0	1	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	('0')	java.lang.Boolean	1	1	10	DATATYPES	ISAUTOINCREMENTED	mmuuid:e2f53b86-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	IsCaseSensitive	11	null	boolean	0	1	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	('0')	java.lang.Boolean	1	1	10	DATATYPES	ISCASESENSITIVE	mmuuid:e2f53b87-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	JavaClass	5	null	string	0	500	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	500	500	10	DATATYPES	JAVACLASS	mmuuid:e3e95f82-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	UID	15	null	string	0	50	true	true	false	false	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	DATATYPES	UID	mmuuid:e3e95f83-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	Precision	12	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	DATATYPES	PRECISION	mmuuid:e3e95f84-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	Radix	13	null	integer	0	10	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	DATATYPES	RADIX	mmuuid:e3e95f85-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	SearchType	14	null	string	0	20	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	20	20	10	DATATYPES	SEARCHTYPE	mmuuid:e3e95f87-b80e-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	Description	18	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	DATATYPES	DESCRIPTION	mmuuid:e81d8400-3a83-1edb-8606-be949cc6da52	null", //$NON-NLS-1$
				"System	DataTypes	System.DataTypes	BaseType	17	null	string	0	64	true	true	false	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	64	64	10	DATATYPES	BASETYPE	mmuuid:e8729800-e584-1e20-b806-d8b2a0a91d66	null", //$NON-NLS-1$
				"System	Elements	System.Elements	GroupUpperName	27	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTS	GROUPUPPERNAME	mmuuid:e92fbb00-c2a0-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	Elements	System.Elements	UpperName	28	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTS	UPPERNAME	mmuuid:ea23df00-c2a0-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	KeyElements	System.KeyElements	Name	3	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYELEMENTS	NAME	mmuuid:ed887840-c318-1de7-b515-bad6cb0abb8d	null", //$NON-NLS-1$
				"System	Groups	System.Groups	IsSystem	12	null	boolean	0	1	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	GROUPS	ISSYSTEM	mmuuid:ee427900-7eec-1eea-940e-bcb0b71c723a	null", //$NON-NLS-1$
				"System	ProcedureParams	System.ProcedureParams	NullType	12	null	string	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	10	10	10	PROCEDUREPARAMS	NULLTYPE	mmuuid:f8acf880-634e-1e44-a903-c1472e78d1c5	null", //$NON-NLS-1$
				"PartsSupplier	PARTS	PartsSupplier.PARTSSUPPLIER.PARTS	PART_ID	1	PART_ID	string	0	4	true	true	true	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	0	4	10	PARTS	PART_ID	mmuuid:fadcd7c0-73fe-1edc-a81c-ecf397b10590	null", //$NON-NLS-1$
				"System	Keys	System.Keys	ModelName	1	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYS	MODELNAME	mmuuid:fe2c6482-b9eb-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Keys	System.Keys	GroupName	8	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYS	GROUPNAME	mmuuid:fe2c6483-b9eb-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Keys	System.Keys	GroupFullName	2	null	string	0	2048	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	2048	2048	10	KEYS	GROUPFULLNAME	mmuuid:fe2c6484-b9eb-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Keys	System.Keys	Name	3	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYS	NAME	mmuuid:fe2c6485-b9eb-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Keys	System.Keys	Description	4	null	string	0	255	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYS	DESCRIPTION	mmuuid:fe2c6486-b9eb-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Keys	System.Keys	IsIndexed	7	null	boolean	0	1	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Boolean	1	1	10	KEYS	ISINDEXED	mmuuid:ff208881-b9eb-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Keys	System.Keys	RefKeyUID	10	null	string	0	50	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	50	50	10	KEYS	REFKEYUID	mmuuid:ff208882-b9eb-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$
				"System	Keys	System.Keys	UID	11	null	string	0	50	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	50	50	10	KEYS	UID	mmuuid:ff208884-b9eb-1de7-b705-ef2376c3949e	null", //$NON-NLS-1$

		};
		executeAndAssertResults("select* from System.Elements order by UID", //$NON-NLS-1$
				expected);
	}

	@Test public void testGroupType() {

		String[] expected = { "Type[string]	", "Table", }; //$NON-NLS-1$ //$NON-NLS-2$
		executeAndAssertResults(
				"select distinct Type from System.Groups order by Type", //$NON-NLS-1$
				expected);
	}

	@Test public void testGroupIsSystem() {

		String[] expected = { "Name[string]	", "PARTS", "SHIP_VIA", "STATUS", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
				"SUPPLIER", "SUPPLIER_PARTS", }; //$NON-NLS-1$ //$NON-NLS-2$
		executeAndAssertResults(
				"select Name from System.Groups where IsSystem = 'false' order by Name", //$NON-NLS-1$
				expected);
	}

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

	@Test public void testOATYPES() {
		String[] expected = { "TYPE_NAME[string]	DATA_TYPE[short]	PRECISION[integer]	LITERAL_PREFIX[string]	LITERAL_SUFFIX[string]	CREATE_PARAMS[string]	NULLABLE[short]	CASE_SENSITIVE[short]	SEARCHABLE[short]	UNSIGNED_ATTRIBUTE[short]	MONEY[short]	AUTO_INCREMENT[short]	LOCAL_TYPE_NAME[string]	MINIMUM_SCALE[short]	MAXIMUM_SCALE[short]", }; //$NON-NLS-1$

		executeAndAssertResults(
				"select TYPE_NAME, DATA_TYPE, OA_PRECISION as PRECISION, LITERAL_PREFIX, LITERAL_SUFFIX, " //$NON-NLS-1$
						+ " CREATE_PARAMS, OA_NULLABLE as NULLABLE, CASE_SENSITIVE, OA_SEARCHABLE as SEARCHABLE, " //$NON-NLS-1$
						+ " UNSIGNED_ATTRIB as UNSIGNED_ATTRIBUTE, OA_MONEY as MONEY, AUTO_INCREMENT, LOCAL_TYPE_NAME, " //$NON-NLS-1$
						+ " MINIMUM_SCALE, MAXIMUM_SCALE  FROM System.ODBC.OA_TYPES  WHERE DATA_TYPE = -6", //$NON-NLS-1$
				expected);
	}

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

	@Test public void testDefect12054() {

		String[] expected = {
				"PKTABLE_QUALIFIER[string]	PKTABLE_OWNER[string]	PKTABLE_NAME[string]	PKCOLUMN_NAME[string]	FKTABLE_QUALIFIER[string]	FKTABLE_OWNER[string]	FKTABLE_NAME[string]	FKCOLUMN_NAME[string]	KEY_SEQ[short]	UPDATE_RULE[short]	DELETE_RULE[short]	FK_NAME[string]	PK_NAME[string]", //$NON-NLS-1$
				"		PARTS	PART_ID			SUPPLIER_PARTS	PART_ID	1	null	null	FK_SPLIER_PRTS_PRTS	PK_PARTS", //$NON-NLS-1$
				"		SHIP_VIA	SHIPPER_ID			null	null	1	null	null	null	PK_SHIP_VIA", //$NON-NLS-1$
				"		STATUS	STATUS_ID			SUPPLIER	SUPPLIER_STATUS	1	null	null	FK_SPLIER_STATS	PK_STATUS", //$NON-NLS-1$
				"		SUPPLIER	SUPPLIER_ID			SUPPLIER_PARTS	SUPPLIER_ID	1	null	null	FK_SPLY_PRTS_SPLY	PK_SUPPLIER", //$NON-NLS-1$
				"		SUPPLIER	SUPPLIER_STATUS			null	null	1	null	null	null	FK_SPLIER_STATS", //$NON-NLS-1$
				"		SUPPLIER_PARTS	PART_ID			null	null	2	null	null	null	PK_SUPPLIER_PARTS", //$NON-NLS-1$
				"		SUPPLIER_PARTS	SUPPLIER_ID			null	null	1	null	null	null	PK_SUPPLIER_PARTS", //$NON-NLS-1$
				"		SUPPLIER_PARTS	PART_ID			null	null	1	null	null	null	FK_SPLIER_PRTS_PRTS", //$NON-NLS-1$
				"		SUPPLIER_PARTS	SUPPLIER_ID			null	null	1	null	null	null	FK_SPLY_PRTS_SPLY", //$NON-NLS-1$

		};

		executeAndAssertResults(
				"select '' AS PKTABLE_QUALIFIER, '' AS PKTABLE_OWNER, PK.GroupName AS PKTABLE_NAME, PK.Name AS PKCOLUMN_NAME, '' AS FKTABLE_QUALIFIER, '' AS FKTABLE_OWNER, FK.GroupName AS FKTABLE_NAME, FK.Name AS FKCOLUMN_NAME, convert(PK.Position, short) AS KEY_SEQ, convert(null, short) AS UPDATE_RULE, convert(null, short) AS DELETE_RULE, FK.KeyName AS FK_NAME, PK.KeyName AS PK_NAME FROM System.KeyElements AS PK LEFT OUTER JOIN System.KeyElements AS FK ON FK.RefKeyUID = PK.UID order by PKTABLE_NAME", //$NON-NLS-1$
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
		executeAndAssertResults("select KeyName, RefKeyUID FROM System.KeyElements WHERE RefKeyUID IS NULL order by KeyName",expected); //$NON-NLS-1$
	}

	@Test public void testSlowSystemQuery() {
		String[] expected = { "groupFullName[string]	SOURCE_NAME[string]	INFO_CAT_NAME[string]	COMMON_SCHEMA_FLAG[string]", }; //$NON-NLS-1$
		executeAndAssertResults(
				"select sgp1.groupFullName, sgp1.groupname AS SOURCE_NAME, sgp1.value AS INFO_CAT_NAME, sgp3.value AS COMMON_SCHEMA_FLAG from System.GroupProperties sgp1, System.GroupProperties sgp2, System.GroupProperties sgp3 where sgp1.groupFullName = sgp2.groupFullName and sgp1.groupFullName = sgp3.groupFullName and sgp2.groupFullName = sgp3.groupFullName and sgp1.name='InformationCategory' and sgp2.name = 'presentationMetadataFlag' and sgp2.value = 'true' and sgp3.name = 'infoCatCommonSchemaFlag'", //$NON-NLS-1$
				expected);
	}

	@Test public void testReferenceKeyColumns() {
		String[] expected = {
				"PKTABLE_CAT[string]	PKTABLE_SCHEM[string]	PKTABLE_NAME[string]	PKCOLUMN_NAME[string]	FKTABLE_CAT[string]	FKTABLE_SCHEM[string]	FKTABLE_NAME[string]	FKCOLUMN_NAME[string]	KEY_SEQ[short]	UPDATE_RULE[integer]	DELETE_RULE[integer]	FK_NAME[string]	PK_NAME[string]	DEFERRABILITY[integer]", //$NON-NLS-1$
				"null	PartsSupplier	PartsSupplier.PARTSSUPPLIER.PARTS	PART_ID	null	PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	PART_ID	1	3	3	FK_SPLIER_PRTS_PRTS	PK_PARTS	5",  //$NON-NLS-1$
				"null	PartsSupplier	PartsSupplier.PARTSSUPPLIER.STATUS	STATUS_ID	null	PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER	SUPPLIER_STATUS	1	3	3	FK_SPLIER_STATS	PK_STATUS	5", //$NON-NLS-1$
				"null	PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER	SUPPLIER_ID	null	PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	SUPPLIER_ID	1	3	3	FK_SPLY_PRTS_SPLY	PK_SUPPLIER	5" //$NON-NLS-1$
		};

		executeAndAssertResults("select* FROM System.JDBC.ReferenceKeyColumns order by PKTABLE_NAME", //$NON-NLS-1$
				expected);
	}
	
	@Test public void testVirtualLookup() {
		String[] expected = { "expr[string]", "null"}; //$NON-NLS-1$ //$NON-NLS-2$
		executeAndAssertResults(
				"select lookup('System.KeyElements', 'RefKeyUID', 'KeyName', 'PK_PARTS')", expected); //$NON-NLS-1$
		
	}
}
