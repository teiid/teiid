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
package org.teiid.deployers;

import org.teiid.core.CoreConstants;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.Type;
import org.teiid.translator.TranslatorException;

public class PgCatalogMetadataStore {

	private static final long serialVersionUID = 5391872008395637166L;
	private MetadataFactory factory;
	
	public PgCatalogMetadataStore(MetadataFactory factory) throws TranslatorException {
		this.factory = factory;
		
		Schema schema = factory.getMetadataStore().getSchemas().get(CoreConstants.ODBC_MODEL);
		schema.setUUID("mmuuid:8294601c-9fe9-4244-9499-4a012c5e1476"); //$NON-NLS-1$
		
		add_pg_namespace();			
		add_pg_class();			
		add_pg_attribute();
		add_pg_type();			
		add_pg_index();
		add_pg_am();
		add_pg_proc();
		add_pg_trigger();
		add_pg_attrdef();
		add_pg_database();
		add_pg_user();
	}
	
	public MetadataStore getMetadataStore()  {
		return factory.getMetadataStore();
	}

	private Table createView(String name, String uuid) throws TranslatorException {
		Table t = this.factory.addTable(name);
		t.setSystem(true);
		t.setSupportsUpdate(false);
		t.setVirtual(true);
		t.setTableType(Type.Table);
		t.setUUID(uuid);
		return t;
	}
	
	private void addColumn(String name, String type, ColumnSet<?> table, String uuid) throws TranslatorException {
		Column c = this.factory.addColumn(name, type, table);
		c.setUUID(uuid);
	}
	
	//index access methods
	private Table add_pg_am() throws TranslatorException {
		Table t = createView("pg_am", "mmuuid:069bf3d5-79ab-4c78-9ede-b6802e5a0dea"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:644ce6f8-a75f-46e6-a03a-294b02feb6fc"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("amname", DataTypeManager.DefaultDataTypes.STRING, t, "mmuuid:ed5b2740-5024-4c3c-a1ac-9187d0ab16c7"); //$NON-NLS-1$ //$NON-NLS-2$

		String transformation = "SELECT 0 as oid, 'btree' as amname"; //$NON-NLS-1$
		t.setSelectTransformation(transformation);
		
		return t;		
	}

	// column defaul values
	private Table add_pg_attrdef() throws TranslatorException {
		Table t = createView("pg_attrdef", "mmuuid:a54429c7-cc41-4112-982b-df76ef3a507d"); //$NON-NLS-1$ //$NON-NLS-2$
		
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:280c0f10-ed7b-4d36-95a4-1409f22c3839"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("adsrc", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:1f29af59-2a39-4cad-b6ff-986ff224db27"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("adrelid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:54648256-35de-4dd3-9d23-1cf7d14aac1f"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("adnum", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:520aa08f-1341-4e8e-8abd-5785128d79ab"); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation = "SELECT 0 as oid, 0 as adsrc, 0 as adrelid, 0 as adnum"; //$NON-NLS-1$
		t.setSelectTransformation(transformation);
		return t;		
	}
	
	//	table columns ("attributes")
	private Table add_pg_attribute() throws TranslatorException {
		Table t = createView("pg_attribute", "mmuuid:7f89ff91-4ae6-40ff-926d-346c5a61f817"); //$NON-NLS-1$ //$NON-NLS-2$

		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:5142b0b5-b166-4e17-b18c-5bbdb023e1c3"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// OID, The table this column belongs to
		addColumn("attrelid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:5f18b807-9eef-41fa-b6d0-b83e2bf6fa5d"); //$NON-NLS-1$ //$NON-NLS-2$
		// 	The column name
		addColumn("attname", DataTypeManager.DefaultDataTypes.STRING, t, "mmuuid:7099d08f-4206-400e-ba65-fbeeb2066070"); //$NON-NLS-1$ //$NON-NLS-2$
		// OID, The data type of this column
		addColumn("atttypid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:8ef029fe-6410-4c34-8f35-301d25d7bd59"); //$NON-NLS-1$ //$NON-NLS-2$
		// A copy of pg_type.typlen of this column's type 
		addColumn("attlen", DataTypeManager.DefaultDataTypes.SHORT, t, "mmuuid:36973b20-e707-460e-aaa5-ed38f9a1d90a"); //$NON-NLS-1$ //$NON-NLS-2$
		// The number of the column. Ordinary columns are numbered from 1 up. System columns, 
		// such as oid, have (arbitrary) negative numbers 
		addColumn("attnum", DataTypeManager.DefaultDataTypes.SHORT, t, "mmuuid:ca1c8121-21d7-4a19-b009-a0ef482f5657"); //$NON-NLS-1$ //$NON-NLS-2$
		// atttypmod records type-specific data supplied at table creation time (for example, 
		// the maximum length of a varchar column). It is passed to type-specific input functions and 
		// length coercion functions. The value will generally be -1 for types that do not need atttypmod
		addColumn("atttypmod", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:455e727b-8276-4bcd-bd65-9a43b990416a"); //$NON-NLS-1$ //$NON-NLS-2$
		// This represents a not-null constraint. It is possible to change this column to enable or disable the constraint 
		addColumn("attnotnull", DataTypeManager.DefaultDataTypes.BOOLEAN, t, "mmuuid:e9230efa-bde9-49ea-b6da-6ede8c5fb3ee"); //$NON-NLS-1$ //$NON-NLS-2$
		// This column has been dropped and is no longer valid. A dropped column is still physically present in the table, 
		// but is ignored by the parser and so cannot be accessed via SQL
		addColumn("attisdropped", DataTypeManager.DefaultDataTypes.BOOLEAN, t, "mmuuid:910c0c60-63be-44fb-bc30-1ba5528cf471"); //$NON-NLS-1$ //$NON-NLS-2$
		// This column has a default value, in which case there will be a corresponding entry in the pg_attrdef 
		// catalog that actually defines the value 
		addColumn("atthasdef", DataTypeManager.DefaultDataTypes.BOOLEAN, t, "mmuuid:be916cb0-0f48-44d7-ae5c-003822ee3e57"); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation = "SELECT t1.OID as oid, " + //$NON-NLS-1$
				"(SELECT OID FROM SYS.Tables WHERE Name = t1.TableName AND SchemaName = t1.SchemaName) as attrelid, " + //$NON-NLS-1$
				"t1.Name as attname, " + //$NON-NLS-1$
				"(SELECT pt.oid FROM pg_catalog.pg_type pt, SYS.DataTypes dt WHERE (dt.Name = t1.DataType AND dt.Name = pt.typname) " + //$NON-NLS-1$
				"OR ((t1.DataType = 'clob' OR t1.DataType = 'blob') AND pt.typname = 'lo')) as atttypid, " + //$NON-NLS-1$
				"convert(t1.Length, short) as attlen, " + //$NON-NLS-1$
				"convert(t1.Position, short) as attnum, " + //$NON-NLS-1$
				"t1.Length as atttypmod, " + //$NON-NLS-1$
				"false as attnotnull, " + //$NON-NLS-1$
				"false as attisdropped, " + //$NON-NLS-1$
				"false as atthasdef " + //$NON-NLS-1$
				"FROM SYS.Columns as t1"; //$NON-NLS-1$
		t.setSelectTransformation(transformation);
		return t;		
	}
	
	// tables, indexes, sequences ("relations")
	private Table add_pg_class() throws TranslatorException {
		Table t = createView("pg_class", "mmuuid:ad51e389-9443-4a7b-984a-5c1875fbd329"); //$NON-NLS-1$ //$NON-NLS-2$
		
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:2a19b579-70b9-4923-a5df-6bbbbc642042"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// Name of the table, index, view, etc
		addColumn("relname", DataTypeManager.DefaultDataTypes.STRING, t, "mmuuid:e6534fee-6712-4574-b228-2787fb960e46"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// The OID of the namespace that contains this relation (pg_namespace.oid)
		addColumn("relnamespace", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:6881bd36-92b9-4552-b953-028036d59f50"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// r = ordinary table, i = index, S = sequence, v = view, c = composite type, t = TOAST table
		addColumn("relkind", DataTypeManager.DefaultDataTypes.CHAR, t, "mmuuid:0a280f97-8dce-4123-bbc9-54bf3b5fd8f6"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// 	If this is an index, the access method used (B-tree, hash, etc.)
		addColumn("relam", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:6d59ce95-63ef-445d-bf17-6bcb45850623"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// Number of rows in the table. This is only an estimate used by the planner. It is updated 
		// by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX
		addColumn("reltuples", DataTypeManager.DefaultDataTypes.FLOAT, t, "mmuuid:d1b54420-5e09-41e0-a177-181e6a6b94d4"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// Size of the on-disk representation of this table in pages (of size BLCKSZ). This is only an estimate 
		// used by the planner. It is updated by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX
		addColumn("relpages", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:a74a4a3b-20b0-4a62-ac3a-4bd2047979fc"); //$NON-NLS-1$ //$NON-NLS-2$

		// True if table has (or once had) rules; see pg_rewrite catalog 
		addColumn("relhasrules", DataTypeManager.DefaultDataTypes.BOOLEAN, t, "mmuuid:712a6f1e-0c49-48dc-99ed-9f55a7c3d6d4"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// 	True if we generate an OID for each row of the relation
		addColumn("relhasoids", DataTypeManager.DefaultDataTypes.STRING, t, "mmuuid:e099cd49-b50b-4573-a931-9c9eb95d75ae"); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation = "SELECT t1.OID as oid, t1.name as relname, " +  //$NON-NLS-1$
				"(SELECT OID FROM SYS.Schemas WHERE Name = t1.SchemaName) as relnamespace, " + //$NON-NLS-1$
				"convert((CASE t1.isPhysical WHEN true THEN 'r' ELSE 'v' END), char) as relkind," + //$NON-NLS-1$
				"0 as relam, " + //$NON-NLS-1$
				"convert(0, float) as reltuples, " + //$NON-NLS-1$
				"0 as relpages, " + //$NON-NLS-1$
				"false as relhasrules, " + //$NON-NLS-1$
				"false as relhasoids " + //$NON-NLS-1$
				"FROM SYS.Tables t1"; //$NON-NLS-1$
		t.setSelectTransformation(transformation);
		return t;		
	}
	
	// additional index information
	private Table add_pg_index() throws TranslatorException {
		Table t = createView("pg_index", "mmuuid:a3e60b50-8282-4562-81a3-164e2e1481ad"); //$NON-NLS-1$ //$NON-NLS-2$
		
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:f846b870-445f-4263-905b-f43ebdea385e"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// 	The OID of the pg_class entry for this index
		addColumn("indexrelid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:16e440fd-616a-4437-a630-abc2d90c728e"); //$NON-NLS-1$ //$NON-NLS-2$
		// 	The OID of the pg_class entry for the table this index is for
		addColumn("indrelid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:94eed371-461a-47b8-8ca1-0494fa0108fb"); //$NON-NLS-1$ //$NON-NLS-2$
		// 	If true, the table was last clustered on this index
		addColumn("indisclustered", DataTypeManager.DefaultDataTypes.BOOLEAN, t, "mmuuid:bce548a8-ac4a-4c71-a7ab-2ca0235e81ed"); //$NON-NLS-1$ //$NON-NLS-2$
		// If true, this is a unique index
		addColumn("indisunique", DataTypeManager.DefaultDataTypes.BOOLEAN, t, "mmuuid:48d79b76-983b-4291-89c2-41222c2f1296"); //$NON-NLS-1$ //$NON-NLS-2$
		// If true, this index represents the primary key of the table (indisunique should always be true when this is true)
		addColumn("indisprimary", DataTypeManager.DefaultDataTypes.BOOLEAN, t, "mmuuid:37e53c0f-08b4-403f-a8c6-5b28c367b62c"); //$NON-NLS-1$ //$NON-NLS-2$
		// Expression trees (in nodeToString() representation) for index attributes that are not simple 
		// column references. This is a list with one element for each zero entry in indkey. 
		// NULL if all index attributes are simple references
		addColumn("indexprs", DataTypeManager.DefaultDataTypes.STRING, t, "mmuuid:b99ce838-8576-423a-b5b7-e58080e3e65d"); //$NON-NLS-1$ //$NON-NLS-2$
		// This is an array of indnatts values that indicate which table columns this index indexes.
		addColumn("indkey", DataTypeManager.DefaultDataTypes.STRING, t, "mmuuid:4dfcc8eb-a131-4234-993f-051dfa15934e"); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation = "SELECT t1.OID as oid, " + //$NON-NLS-1$
				"t1.OID as indexrelid, " + //$NON-NLS-1$
				"(SELECT OID FROM SYS.Tables WHERE SchemaName = t1.SchemaName AND Name = t1.TableName) as indrelid, " + //$NON-NLS-1$
				"false indisclustered, " + //$NON-NLS-1$
				"(CASE t1.KeyType WHEN 'Unique' THEN true ELSE false END) as indisunique, " + //$NON-NLS-1$
				"(CASE t1.KeyType WHEN 'Primary' THEN true ELSE false END) as indisprimary, " + //$NON-NLS-1$
				"'' as indexprs, " + //$NON-NLS-1$
				"0 as indkey " + //$NON-NLS-1$
				"FROM SYS.KeyColumns as t1"; //$NON-NLS-1$
		t.setSelectTransformation(transformation);
		return t;		
	}

	// schemas
	private Table add_pg_namespace() throws TranslatorException {
		Table t = createView("pg_namespace", "mmuuid:38438f3b-7664-4449-8f06-be69b2555a4c"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:36d8ca5a-4747-4bb0-8b7b-9f50f6eb9a0e"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("nspname", DataTypeManager.DefaultDataTypes.STRING, t, "mmuuid:8e2c1f9f-359d-4c33-afa5-20fa87585442"); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation = "SELECT t1.OID as oid, t1.Name as nspname " + //$NON-NLS-1$
		"FROM SYS.Schemas as t1"; //$NON-NLS-1$

		t.setSelectTransformation(transformation);

		return t;		
	}
	
	// functions and procedures
	private Table add_pg_proc() throws TranslatorException {
		Table t = createView("pg_proc", "mmuuid:3cffb0db-f326-40e6-890f-9ef7a0980784"); //$NON-NLS-1$ //$NON-NLS-2$

		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:246f56c3-5268-42b8-a486-d3c77653f603"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// Name of the function or procedure
		addColumn("proname", DataTypeManager.DefaultDataTypes.STRING, t, "mmuuid:b9c340ba-bf6f-41ba-aa40-f3c607077280"); //$NON-NLS-1$ //$NON-NLS-2$

		// Function returns a set (i.e., multiple values of the specified data type)
		addColumn("proretset", DataTypeManager.DefaultDataTypes.BOOLEAN, t, "mmuuid:0e2c6601-ecc9-41e2-be0d-3a27565b3714"); //$NON-NLS-1$ //$NON-NLS-2$

		// OID of 	Data type of the return value
		addColumn("prorettype", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:d5e2bf27-a413-4146-a059-37bf651a1b97"); //$NON-NLS-1$ //$NON-NLS-2$

		// Number of input arguments
		addColumn("pronargs", DataTypeManager.DefaultDataTypes.SHORT, t, "mmuuid:1490582b-6223-44df-8b20-9bcd5d241aa8"); //$NON-NLS-1$ //$NON-NLS-2$
		
		addColumn("proargtypes", DataTypeManager.DefaultDataTypes.OBJECT, t, "mmuuid:4af1aac3-5d4c-47c5-ab6c-4a52b5b551b7"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("proargnames", DataTypeManager.DefaultDataTypes.OBJECT, t, "mmuuid:bfc2e071-c49d-40b1-b3e9-d4891af6a77c"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("proargmodes", DataTypeManager.DefaultDataTypes.OBJECT, t, "mmuuid:fd6e0be2-e336-4d3b-8217-f85cfcd0999b"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("proallargtypes", DataTypeManager.DefaultDataTypes.OBJECT, t, "mmuuid:f0e04ef7-1a8b-4007-bce2-e2beba667d53"); //$NON-NLS-1$ //$NON-NLS-2$
		
		// The OID of the namespace that contains this function 
		addColumn("pronamespace", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:458f5136-4128-47e8-8063-3c97fb1add5c"); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation = "SELECT t1.OID as oid, t1.Name as proname, false as proretset, " + //$NON-NLS-1$
				"(SELECT dt.OID FROM ProcedureParams pp, DataTypes dt WHERE pp.ProcedureName = t1.Name AND pp.SchemaName = t1.SchemaName AND pp.Type = 'ResultSet' AND pp.Position = 1 AND dt.Name = pp.DataType) as prorettype, " + //$NON-NLS-1$ 
				"(SELECT count(*) FROM ProcedureParams pp WHERE pp.ProcedureName = t1.Name AND pp.SchemaName = t1.SchemaName ) as pronargs, " + //$NON-NLS-1$
				"null as proargtypes, " + //$NON-NLS-1$
				"null as proargnames, " + //$NON-NLS-1$
				"null as proargmodes, " + //$NON-NLS-1$
				"null as proallargtypes, " + //$NON-NLS-1$
				"(SELECT OID FROM SYS.Schemas WHERE Name = t1.SchemaName) as pronamespace " + //$NON-NLS-1$
				"FROM SYS.Procedures as t1"; //$NON-NLS-1$
		t.setSelectTransformation(transformation);
		return t;		
	}

	
	// triggers
	private Table add_pg_trigger() throws TranslatorException  {
		Table t = createView("pg_trigger", "mmuuid:dbdacb28-7e78-4ae5-8a99-3e3e1c59f641"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:7b1632d0-8357-47d6-9233-24291059f37d"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("tgconstrrelid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:f78e6a3c-5c27-4381-9d76-870fd3b6b510"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("tgfoid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:9a9d2791-506e-4e06-88e2-fadb90cb8d8c"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("tgargs", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:6ab14a63-9aa3-416c-872b-8f6a37131fa4"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("tgnargs", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:ec283b19-42a0-441f-8198-b80f353720d1"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("tgdeferrable", DataTypeManager.DefaultDataTypes.BOOLEAN, t, "mmuuid:264a3677-584c-4ecd-808b-b837acd9c129"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("tginitdeferred", DataTypeManager.DefaultDataTypes.BOOLEAN, t, "mmuuid:034bb072-1571-4953-bc18-216478346304"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("tgconstrname", DataTypeManager.DefaultDataTypes.STRING, t, "mmuuid:ad972784-8dc3-4151-b113-3d95967dc19f"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("tgrelid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:3aecc7ac-d54a-4bf7-be75-f64b37eb59ab"); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation = "SELECT 1 as oid, 1 as tgconstrrelid, " +//$NON-NLS-1$
				"1 as tgfoid, " +//$NON-NLS-1$
				"1 as tgargs, " +//$NON-NLS-1$
				"1 as tgnargs, " +//$NON-NLS-1$
				"false as tgdeferrable, " +//$NON-NLS-1$
				"false as tginitdeferred, " +//$NON-NLS-1$
				"'dummy' as tgconstrname, " +//$NON-NLS-1$
				"1 as tgrelid " +//$NON-NLS-1$
				"FROM SYS.Tables WHERE 1=2"; //$NON-NLS-1$
		t.setSelectTransformation(transformation);		
		return t;		
	}
	
	//data types
	private Table add_pg_type	() throws TranslatorException {
		Table t = createView("pg_type", "mmuuid:8024e6eb-ba32-41a0-a250-95a36eb4b71f"); //$NON-NLS-1$ //$NON-NLS-2$
		// Data type name
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:83240e67-acd6-49d3-be86-a4e186d110ea"); //$NON-NLS-1$ //$NON-NLS-2$
		// Data type name
		addColumn("typname", DataTypeManager.DefaultDataTypes.STRING, t, "mmuuid:e9f471d8-7ad0-48a9-ab64-7adbe922ff9b"); //$NON-NLS-1$ //$NON-NLS-2$
		// The OID of the namespace that contains this type 
		addColumn("typnamespace", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:e1723124-4e26-4971-a407-ceefaf0f624d"); //$NON-NLS-1$ //$NON-NLS-2$
		// For a fixed-size type, typlen is the number of bytes in the internal representation of the type. 
		// But for a variable-length type, typlen is negative. -1 indicates a "varlena" type (one that 
		// has a length word), -2 indicates a null-terminated C string. 
		addColumn("typlen", DataTypeManager.DefaultDataTypes.SHORT, t, "mmuuid:33b00d6a-aae5-4bcd-80cc-ec7052bdeca2"); //$NON-NLS-1$ //$NON-NLS-2$
		// 	 typtype is b for a base type, c for a composite type (e.g., a table's row type), d for a domain, 
		// e for an enum type, or p for a pseudo-type. See also typrelid and typbasetype
		addColumn("typtype", DataTypeManager.DefaultDataTypes.CHAR, t, "mmuuid:496eb63b-7fb2-4d56-90ac-11e5717acadd"); //$NON-NLS-1$ //$NON-NLS-2$
		// if this is a domain (see typtype), then typbasetype identifies the type that this one is based on. 
		// Zero if this type is not a domain 
		addColumn("typbasetype", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:e7df7d7a-1ddd-4a56-8514-82272eef5aa3"); //$NON-NLS-1$ //$NON-NLS-2$
		// Domains use typtypmod to record the typmod to be applied to their base type 
		// (-1 if base type does not use a typmod). -1 if this type is not a domain 
		addColumn("typtypmod", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:800d1b55-c775-4a63-b5ed-e76d3b112f60"); //$NON-NLS-1$ //$NON-NLS-2$
		
		addColumn("typrelid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:d44fc440-a677-463f-a403-f04f1896a705"); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation =
			"SELECT 16 as oid,  'boolean' as typname, (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(1, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			//"SELECT 17 as oid,  'blob' as typname,(SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(1, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			//"   union " + //$NON-NLS-1$
			"SELECT 1043 as oid,  'string' as typname,  (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(-1, short) as typlen, convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			"SELECT 25 as oid,  'text' as typname,  (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(-1, short) as typlen, convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			"SELECT 1042 as oid,  'char' as typname,  (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(1, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			"SELECT 21 as oid,  'short' as typname,  (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(2, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			"SELECT 20 as oid,  'long' as typname,  (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(8, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			"SELECT 23 as oid,  'int' as typname,   (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(4, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			"SELECT 26 as oid,  'oid' as typname,  (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typname,    convert(4, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			"SELECT 700 as oid,  'float' as typname,(SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(4, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$ 
			"   union " + //$NON-NLS-1$
			"SELECT 701 as oid,  'double' as typname,  (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(8, short) as typlen, convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			//"SELECT 1009 as oid,  'clob' as typname,(SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(-1, short) as typlen, convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			//"   union " + //$NON-NLS-1$
			"SELECT 1082 as oid,  'date' as typname,  (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(4, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			"SELECT 1083 as oid,  'datetime' as typname,(SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(8, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			"SELECT 1114 as oid,  'timestamp' as typname, (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace, convert(8, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			"SELECT 1700 as oid,  'decimal' as typname, (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(-1, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X"  + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			"SELECT 142 as oid,  'xml' as typname, (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(-1, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
			"   union " + //$NON-NLS-1$
			"SELECT 14939 as oid,  'lo' as typname, (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(-1, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X"; //$NON-NLS-1$
		
		t.setSelectTransformation(transformation);			
		return t;		
	}
	
	private Table add_pg_database() throws TranslatorException  {
		Table t = createView("pg_database", "mmuuid:6ae73c29-0c6f-4ec3-9c09-a262d8e41ac2"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:a2be6d15-b504-4257-962e-2c3fa90e3c16"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("datname", DataTypeManager.DefaultDataTypes.STRING, t, "mmuuid:e751d595-6afe-430e-9f57-a56cee474765"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("encoding", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:637b6fec-e56a-461b-b714-7c88976d7cde"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("datlastsysoid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:c93379b9-1b4d-4068-890f-deed28338a54"); //$NON-NLS-1$ //$NON-NLS-2$
		// this is is boolean type but the query coming in is in the form dataallowconn = 't'
		addColumn("datallowconn", DataTypeManager.DefaultDataTypes.CHAR, t, "mmuuid:7be945c4-6bca-4f65-b655-b055b6d31c56"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("datconfig", DataTypeManager.DefaultDataTypes.OBJECT, t, "mmuuid:dde7619f-7071-490f-85c8-ba8c68cb0e78"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("datacl", DataTypeManager.DefaultDataTypes.OBJECT, t, "mmuuid:80ed5897-0b9c-4b3a-95ee-0e5ff4faad34"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("datdba", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:1190357d-63d4-4b19-a0cd-f9f23b11e23e"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("dattablespace", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:6261ae76-3d53-4bda-bb5c-a353818292ae"); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation = "SELECT 0 as oid, " + //$NON-NLS-1$
				"'teiid' as datname, " + //$NON-NLS-1$
				"6 as encoding, " + //$NON-NLS-1$
				"100000 as datlastsysoid, " + //$NON-NLS-1$
				"convert('t', char) as datallowconn, " + //$NON-NLS-1$
				"null, " + //$NON-NLS-1$
				"null, " + //$NON-NLS-1$
				"0 as datdba, " + //$NON-NLS-1$
				"0 as dattablespace" ; //$NON-NLS-1$
		t.setSelectTransformation(transformation);	
		return t;
	}
	
	private Table add_pg_user() throws TranslatorException  {
		Table t = createView("pg_user", "mmuuid:0da462b7-bacf-41da-9335-9a12224c462a"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t, "mmuuid:dcebd4db-ba52-4909-ab57-528c818e94b7"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("usename", DataTypeManager.DefaultDataTypes.STRING, t, "mmuuid:8d148996-16a1-44d8-b5ff-06f9421415d4"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("usecreatedb", DataTypeManager.DefaultDataTypes.BOOLEAN, t, "mmuuid:7f20dc11-f376-4da5-9fe5-139c2562b4c2"); //$NON-NLS-1$ //$NON-NLS-2$
		addColumn("usesuper", DataTypeManager.DefaultDataTypes.BOOLEAN, t, "mmuuid:f3434529-3e9a-4f11-90c0-b74374947902"); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation = "SELECT 0 as oid, " + //$NON-NLS-1$
				"null as usename, " + //$NON-NLS-1$
				"false as usecreatedb, " + //$NON-NLS-1$
				"false as usesuper "; //$NON-NLS-1$
		t.setSelectTransformation(transformation);	
		return t;
	}
}
