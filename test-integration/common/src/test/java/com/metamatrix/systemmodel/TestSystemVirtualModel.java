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
import org.junit.Ignore;
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

	@Test public void testModels() {

		String[] expected = {
				"Name[string]	IsPhysical[boolean]	UID[string]	Description[string]	PrimaryMetamodelURI[string]", //$NON-NLS-1$
				"PartsSupplier	true	mmuuid:19c7f380-73d8-1edc-a81c-ecf397b10590	null	http://www.metamatrix.com/metamodels/Relational", //$NON-NLS-1$
				"System	true	mmuuid:49b9c0f9-2c4c-42d3-9409-2d847d29a1de	System	http://www.metamatrix.com/metamodels/Relational", //$NON-NLS-1$
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
				"System	System.Procedures	Procedures	Table	null	true	PROCEDURES	false	mmuuid:0bc132a5-9f8d-4a3c-9f5d-98156a98a962	0	null	true	false", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SHIP_VIA	SHIP_VIA	Table	SHIP_VIA	true	SHIP_VIA	true	mmuuid:0f4e9b80-73ff-1edc-a81c-ecf397b10590	4	null	false	false", //$NON-NLS-1$
				"System	System.KeyElements	KeyElements	Table	null	true	KEYELEMENTS	false	mmuuid:14946083-3bd5-42d5-8283-1c0694347c29	0	null	true	false", //$NON-NLS-1$
				"System	System.Elements	Elements	Table	null	true	ELEMENTS	false	mmuuid:1c9a5cb2-17b1-4e4a-8b0e-3a42bd052509	0	null	true	false", //$NON-NLS-1$
				"System	System.Keys	Keys	Table	null	true	KEYS	false	mmuuid:1e5135dc-ce5d-4b25-a8ff-63f5440b3108	0	null	true	false", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.STATUS	STATUS	Table	STATUS	true	STATUS	true	mmuuid:1f297200-73ff-1edc-a81c-ecf397b10590	3	null	false	false", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER	SUPPLIER	Table	SUPPLIER	true	SUPPLIER	true	mmuuid:2c371ec0-73ff-1edc-a81c-ecf397b10590	16	null	false	false", //$NON-NLS-1$
				"System	System.DataTypeProperties	DataTypeProperties	Table	null	true	DATATYPEPROPERTIES	false	mmuuid:3190c849-907f-4b03-a2ae-3299e52ceee5	0	null	true	false", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.SUPPLIER_PARTS	SUPPLIER_PARTS	Table	SUPPLIER_PARTS	true	SUPPLIER_PARTS	true	mmuuid:3deafb00-73ff-1edc-a81c-ecf397b10590	227	null	false	false", //$NON-NLS-1$
				"System	System.ProcedureParamProperties	ProcedureParamProperties	Table	null	true	PROCEDUREPARAMPROPERTIES	true	mmuuid:415423c0-64bb-46f6-a80b-8ee37d035e52	0	null	true	false", //$NON-NLS-1$
				"System	System.VirtualDatabases	VirtualDatabases	Table	null	true	VIRTUALDATABASES	false	mmuuid:47297c72-d621-4f4e-af4e-74060ac5f489	0	null	true	false", //$NON-NLS-1$
				"System	System.ReferenceKeyColumns	ReferenceKeyColumns	Table	null	true	REFERENCEKEYCOLUMNS	false	mmuuid:6a9653e8-a337-41b2-86fa-77b98f409a29	0	null	true	false", //$NON-NLS-1$
				"System	System.ModelProperties	ModelProperties	Table	null	true	MODELPROPERTIES	false	mmuuid:7a45e50a-d03f-4548-ba35-761651bbca85	0	null	true	false", //$NON-NLS-1$
				"System	System.Groups	Groups	Table	null	true	GROUPS	false	mmuuid:8551b3bd-11cc-4049-9bcf-fe91a0eb7ba7	0	null	true	false", //$NON-NLS-1$
				"System	System.Models	Models	Table	null	true	MODELS	false	mmuuid:8648a554-b2ad-4e8e-84ca-2ec618b311a9	0	null	true	false", //$NON-NLS-1$
				"System	System.KeyProperties	KeyProperties	Table	null	true	KEYPROPERTIES	false	mmuuid:921e056b-f658-439f-9ab1-cf999c0f3736	0	null	true	false", //$NON-NLS-1$
				"System	System.ProcedureProperties	ProcedureProperties	Table	null	true	PROCEDUREPROPERTIES	false	mmuuid:9220aa98-e34c-4a7e-b747-e7952a45842a	0	null	true	false", //$NON-NLS-1$
				"System	System.GroupProperties	GroupProperties	Table	null	true	GROUPPROPERTIES	false	mmuuid:994ff192-9a8a-4ac5-b4f3-138e032df987	0	null	true	false", //$NON-NLS-1$
				"System	System.DataTypes	DataTypes	Table	null	true	DATATYPES	false	mmuuid:9a8794f9-66f8-49e8-8576-89d212d0f957	0	null	true	false", //$NON-NLS-1$
				"System	System.ProcedureParams	ProcedureParams	Table	null	true	PROCEDUREPARAMS	false	mmuuid:a56bd7fe-c87a-411c-8f5d-661975a25626	0	null	true	false", //$NON-NLS-1$
				"System	System.ElementProperties	ElementProperties	Table	null	true	ELEMENTPROPERTIES	false	mmuuid:f1eab72f-0f31-4920-b06b-692e7d6ce023	0	null	true	false", //$NON-NLS-1$
				"PartsSupplier	PartsSupplier.PARTSSUPPLIER.PARTS	PARTS	Table	PARTS	true	PARTS	true	mmuuid:f6276601-73fe-1edc-a81c-ecf397b10590	16	null	false	false", //$NON-NLS-1$
		};
		executeAndAssertResults("select* from System.Groups order by UID", //$NON-NLS-1$
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
		executeAndAssertResults("select * from System.DataTypes order by name", expected); //$NON-NLS-1$

	}

	@Test public void testProcedureParams() {

		String[] expected = {
				"ModelName[string]	ProcedureName[string]	Name[string]	DataType[string]	Position[integer]	Type[string]	Optional[boolean]	Precision[integer]	TypeLength[integer]	Scale[integer]	Radix[integer]	NullType[string]	UID[string]", //$NON-NLS-1$
  	            "System	System.getVDBResourcePaths	ResourcePath	string	1	ResultSet	false	50	50	0	10	Nullable	mmuuid:ebbffdab-ac7e-41ab-974f-62785b3086f9", //$NON-NLS-1$
  	            "System	System.getBinaryVDBResource	VdbResource	blob	1	ResultSet	false	0	0	0	10	Nullable	mmuuid:90d1f79d-bd98-46f4-ae8f-adacc329cf0b", //$NON-NLS-1$
  	            "System	System.getCharacterVDBResource	VdbResource	clob	1	ResultSet	false	0	0	0	10	Nullable	mmuuid:111f9fa8-74c9-479a-a184-8db64a1eab3c", //$NON-NLS-1$
  	            "System	System.getVDBResourcePaths	isBinary	boolean	2	ResultSet	false	1	1	0	10	Nullable	mmuuid:e8d087da-9833-4422-a255-f0e0fea2cb61", //$NON-NLS-1$
  	            "System	System.getBinaryVDBResource	resourcePath	string	1	In	false	50	50	0	10	No Nulls	mmuuid:25e5065a-454e-4a87-bf71-b6f71b98319f", //$NON-NLS-1$
  	            "System	System.getCharacterVDBResource	resourcePath	string	1	In	false	50	50	0	10	No Nulls	mmuuid:c54e777f-3cd0-45ad-a18b-e4e93532984f", //$NON-NLS-1$

		};
		executeAndAssertResults(
				"select * from System.ProcedureParams order by Name", expected); //$NON-NLS-1$
	}

	@Test public void testProcedures() {

		String[] expected = {
				"ModelName[string]	Name[string]	NameInSource[string]	ReturnsResults[boolean]	ModelUID[string]	UID[string]	Description[string]	FullName[string]", //$NON-NLS-1$
				"System	getBinaryVDBResource	null	true	mmuuid:49b9c0f9-2c4c-42d3-9409-2d847d29a1de	mmuuid:9bc0b701-b36e-4209-a986-9d38420d2c20	null	System.getBinaryVDBResource", //$NON-NLS-1$
				"System	getCharacterVDBResource	null	true	mmuuid:49b9c0f9-2c4c-42d3-9409-2d847d29a1de	mmuuid:72464082-00fc-44f0-98b9-8c8f637c6570	null	System.getCharacterVDBResource", //$NON-NLS-1$
				"System	getVDBResourcePaths	null	true	mmuuid:49b9c0f9-2c4c-42d3-9409-2d847d29a1de	mmuuid:1204d4b2-6f92-428d-bcc5-7b3a0da9a113	null	System.getVDBResourcePaths", //$NON-NLS-1$
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
  	            "System	DataTypes	System.DataTypes	BaseType	17	null	string	0	64	true	true	false	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	64	64	10	DATATYPES	BASETYPE	mmuuid:03beb57c-968b-4821-a6ae-cb1154cfadee	null", //$NON-NLS-1$
  	            "System	Groups	System.Groups	Cardinality	10	null	integer	0	10	false	true	false	true	true	false	false	No Nulls	null	null	All Except Like	null	null	java.lang.Integer	10	10	10	GROUPS	CARDINALITY	mmuuid:24cdad3a-e8f7-4376-bb32-79f8bc8eeed2	null", //$NON-NLS-1$
  	            "System	Elements	System.Elements	CharOctetLength	25	null	integer	0	10	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	ELEMENTS	CHAROCTETLENGTH	mmuuid:de5def94-2804-4c91-91ed-26d630ce8afe	null", //$NON-NLS-1$
  	            "System	ReferenceKeyColumns	System.ReferenceKeyColumns	DEFERRABILITY	14	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	REFERENCEKEYCOLUMNS	DEFERRABILITY	mmuuid:88380f55-2cbd-4325-b9a3-9dcaa88a690e	null", //$NON-NLS-1$
  	            "System	ReferenceKeyColumns	System.ReferenceKeyColumns	DELETE_RULE	11	null	integer	0	10	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.Integer	10	10	10	REFERENCEKEYCOLUMNS	DELETE_RULE	mmuuid:9207f4df-a5ce-43bd-b3b2-fee57e459849	null", //$NON-NLS-1$
  	            "System	DataTypeProperties	System.DataTypeProperties	DataType	1	null	string	0	100	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	100	100	10	DATATYPEPROPERTIES	DATATYPE	mmuuid:b9b99be5-1472-4499-84a8-031caf7efee7	null", //$NON-NLS-1$
  	            "System	Elements	System.Elements	DataType	7	null	string	0	100	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	100	100	10	ELEMENTS	DATATYPE	mmuuid:9a8dc0d5-e65c-4032-a066-187f8d2e73ea	null", //$NON-NLS-1$
  	            "System	ProcedureParams	System.ProcedureParams	DataType	4	null	string	0	25	true	true	false	true	true	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	25	25	10	PROCEDUREPARAMS	DATATYPE	mmuuid:207a09af-65b8-405f-b1cb-537bc8632fa4	null", //$NON-NLS-1$
  	            "System	Elements	System.Elements	DefaultValue	22	null	string	0	255	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTS	DEFAULTVALUE	mmuuid:7e853988-356b-4c7c-83d4-a9f015bff279	null", //$NON-NLS-1$
  	            "System	DataTypes	System.DataTypes	Description	18	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	DATATYPES	DESCRIPTION	mmuuid:b7d95ef6-63a3-441c-8de5-c98e2e577ea3	null", //$NON-NLS-1$
  	            "System	Elements	System.Elements	Description	30	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTS	DESCRIPTION	mmuuid:74d73b53-b723-419e-9fea-de56408409ee	null", //$NON-NLS-1$
  	            "System	Groups	System.Groups	Description	11	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	GROUPS	DESCRIPTION	mmuuid:51605e41-5cb0-40ca-8c4a-4eca52780afc	null", //$NON-NLS-1$
  	            "System	Keys	System.Keys	Description	4	null	string	0	255	true	true	false	false	false	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	KEYS	DESCRIPTION	mmuuid:175e21b2-24c3-4677-a253-6d7cdb513a9a	null", //$NON-NLS-1$
  	            "System	Models	System.Models	Description	4	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	MODELS	DESCRIPTION	mmuuid:1cb99300-a527-4a26-b4e6-08ebd92a781d	null", //$NON-NLS-1$
  	            "System	Procedures	System.Procedures	Description	7	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	225	255	10	PROCEDURES	DESCRIPTION	mmuuid:fa0b5db7-acb1-4975-8410-d5d27df46040	null", //$NON-NLS-1$
  	            "System	Elements	System.Elements	ElementLength	9	null	integer	0	10	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.Integer	10	10	10	ELEMENTS	ELEMENTLENGTH	mmuuid:b36ea0f6-cbff-4049-bc9c-8ec9928be048	null", //$NON-NLS-1$
  	            "System	ElementProperties	System.ElementProperties	ElementName	3	null	string	0	255	true	true	false	false	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTPROPERTIES	ELEMENTNAME	mmuuid:7d8118bd-909a-42be-a0e2-30e668c778cf	null", //$NON-NLS-1$
  	            "System	ElementProperties	System.ElementProperties	ElementUpperName	7	null	string	0	255	true	true	false	true	false	false	false	No Nulls	null	null	Searchable	null	null	java.lang.String	255	255	10	ELEMENTPROPERTIES	ELEMENTUPPERNAME	mmuuid:ec7d4d7d-9514-448d-afd7-d72c95dd732d	null", //$NON-NLS-1$
  	            "System	ReferenceKeyColumns	System.ReferenceKeyColumns	FKCOLUMN_NAME	8	null	string	0	255	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	255	255	10	REFERENCEKEYCOLUMNS	FKCOLUMN_NAME	mmuuid:f4b2b32c-e411-45e6-a236-fec4718f0874	null", //$NON-NLS-1$    	                 	                  	                        	              	                      	              
  	            "System	ReferenceKeyColumns	System.ReferenceKeyColumns	FKTABLE_CAT	5	null	string	0	1	false	true	true	true	true	false	false	Nullable	null	null	Searchable	null	null	java.lang.String	1	1	10	REFERENCEKEYCOLUMNS	FKTABLE_CAT	mmuuid:a0095da3-1258-44dc-bab9-33eacf886a28	null", //$NON-NLS-1$
  	            
		};
		executeAndAssertResults("select* from System.Elements order by Name limit 20", //$NON-NLS-1$
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

	@Test public void testDefect12054() {

		String[] expected = {
				"PKTABLE_QUALIFIER[string]	PKTABLE_OWNER[string]	PKTABLE_NAME[string]	PKCOLUMN_NAME[string]	FKTABLE_QUALIFIER[string]	FKTABLE_OWNER[string]	FKTABLE_NAME[string]	FKCOLUMN_NAME[string]	KEY_SEQ[short]	UPDATE_RULE[short]	DELETE_RULE[short]	FK_NAME[string]	PK_NAME[string]", //$NON-NLS-1$
				"		PARTS	PART_ID			SUPPLIER_PARTS	PART_ID	1	null	null	FK_SPLIER_PRTS_PRTS	PK_PARTS", //$NON-NLS-1$
				"		SHIP_VIA	SHIPPER_ID			null	null	1	null	null	null	PK_SHIP_VIA", //$NON-NLS-1$
				"		STATUS	STATUS_ID			SUPPLIER	SUPPLIER_STATUS	1	null	null	FK_SPLIER_STATS	PK_STATUS", //$NON-NLS-1$
				"		SUPPLIER	SUPPLIER_ID			SUPPLIER_PARTS	SUPPLIER_ID	1	null	null	FK_SPLY_PRTS_SPLY	PK_SUPPLIER", //$NON-NLS-1$
				"		SUPPLIER	SUPPLIER_STATUS			null	null	1	null	null	null	FK_SPLIER_STATS", //$NON-NLS-1$
				"		SUPPLIER_PARTS	SUPPLIER_ID			null	null	1	null	null	null	PK_SUPPLIER_PARTS", //$NON-NLS-1$
				"		SUPPLIER_PARTS	PART_ID			null	null	2	null	null	null	PK_SUPPLIER_PARTS", //$NON-NLS-1$
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

		executeAndAssertResults("select* FROM System.ReferenceKeyColumns order by PKTABLE_NAME", //$NON-NLS-1$
				expected);
	}
	
	@Test public void testVirtualLookup() {
		String[] expected = { "expr[string]", "null"}; //$NON-NLS-1$ //$NON-NLS-2$
		executeAndAssertResults(
				"select lookup('System.KeyElements', 'RefKeyUID', 'KeyName', 'PK_PARTS')", expected); //$NON-NLS-1$
		
	}
}
