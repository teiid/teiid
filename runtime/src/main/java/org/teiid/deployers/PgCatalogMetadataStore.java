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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.Type;
import org.teiid.translator.TranslatorException;

public class PgCatalogMetadataStore extends MetadataFactory {

	private static final long serialVersionUID = 5391872008395637166L;
	private Random random;
	
	public PgCatalogMetadataStore(String modelName, Map<String, Datatype> dataTypes, Properties importProperties) throws TranslatorException {
		super(modelName, dataTypes, importProperties);
		
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
	
	@Override
	protected void setUUID(AbstractMetadataRecord record) {
        byte[] randomBytes = new byte[8];
        if (random == null) {
        	random = new Random(2010);
        }
        random.nextBytes(randomBytes);
        randomBytes[6]  &= 0x0f;  /* clear version        */
        randomBytes[6]  |= 0x40;  /* set to version 4     */
        long msb = new BigInteger(randomBytes).longValue();
        random.nextBytes(randomBytes);
        randomBytes[0]  &= 0x3f;  /* clear variant        */
        randomBytes[0]  |= 0x80;  /* set to IETF variant  */
        long lsb = new BigInteger(randomBytes).longValue();
        record.setUUID("mmuid:"+new UUID(msb, lsb)); //$NON-NLS-1$
	}
	
	private Table createView(String name) throws TranslatorException {
		Table t = addTable(name);
		t.setSystem(true);
		t.setSupportsUpdate(false);
		t.setVirtual(true);
		t.setTableType(Type.Table);
		return t;
	}
	
	//index access methods
	private Table add_pg_am() throws TranslatorException {
		Table t = createView("pg_am"); //$NON-NLS-1$ 
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("amname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 

		String transformation = "SELECT 0 as oid, 'btree' as amname"; //$NON-NLS-1$
		t.setSelectTransformation(transformation);
		
		return t;		
	}

	// column defaul values
	private Table add_pg_attrdef() throws TranslatorException {
		Table t = createView("pg_attrdef"); //$NON-NLS-1$ 
		
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("adsrc", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("adrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("adnum", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
		String transformation = "SELECT 0 as oid, 0 as adsrc, 0 as adrelid, 0 as adnum"; //$NON-NLS-1$
		t.setSelectTransformation(transformation);
		return t;		
	}
	
	//	table columns ("attributes")
	private Table add_pg_attribute() throws TranslatorException {
		Table t = createView("pg_attribute"); //$NON-NLS-1$ 

		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
		// OID, The table this column belongs to
		addColumn("attrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		// 	The column name
		addColumn("attname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 
		// OID, The data type of this column
		addColumn("atttypid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		// A copy of pg_type.typlen of this column's type 
		addColumn("attlen", DataTypeManager.DefaultDataTypes.SHORT, t); //$NON-NLS-1$ 
		// The number of the column. Ordinary columns are numbered from 1 up. System columns, 
		// such as oid, have (arbitrary) negative numbers 
		addColumn("attnum", DataTypeManager.DefaultDataTypes.SHORT, t); //$NON-NLS-1$ 
		// atttypmod records type-specific data supplied at table creation time (for example, 
		// the maximum length of a varchar column). It is passed to type-specific input functions and 
		// length coercion functions. The value will generally be -1 for types that do not need atttypmod
		addColumn("atttypmod", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		// This represents a not-null constraint. It is possible to change this column to enable or disable the constraint 
		addColumn("attnotnull", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 
		// This column has been dropped and is no longer valid. A dropped column is still physically present in the table, 
		// but is ignored by the parser and so cannot be accessed via SQL
		addColumn("attisdropped", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 
		// This column has a default value, in which case there will be a corresponding entry in the pg_attrdef 
		// catalog that actually defines the value 
		addColumn("atthasdef", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 
		
		addPrimaryKey("pk_pg_attr", Arrays.asList("oid"), t); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation = "SELECT t1.OID as oid, " + //$NON-NLS-1$
				"st.oid as attrelid, " + //$NON-NLS-1$
				"t1.Name as attname, " + //$NON-NLS-1$
				"pt.oid as atttypid," + //$NON-NLS-1$
				"pt.typlen as attlen, " + //$NON-NLS-1$
				"convert(t1.Position, short) as attnum, " + //$NON-NLS-1$
				"t1.Length as atttypmod, " + //$NON-NLS-1$
				"false as attnotnull, " + //$NON-NLS-1$
				"false as attisdropped, " + //$NON-NLS-1$
				"false as atthasdef " + //$NON-NLS-1$
				"FROM SYS.Columns as t1 LEFT OUTER JOIN " + //$NON-NLS-1$
				"SYS.Tables st ON (st.Name = t1.TableName AND st.SchemaName = t1.SchemaName) LEFT OUTER JOIN " + //$NON-NLS-1$
				"pg_catalog.pg_type pt ON (CASE WHEN (t1.DataType = 'clob' OR t1.DataType = 'blob') THEN 'lo' ELSE t1.DataType END = pt.typname)"; //$NON-NLS-1$
		t.setSelectTransformation(transformation);
		t.setMaterialized(true);
		return t;		
	}
	
	// tables, indexes, sequences ("relations")
	private Table add_pg_class() throws TranslatorException {
		Table t = createView("pg_class"); //$NON-NLS-1$ 
		
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
		// Name of the table, index, view, etc
		addColumn("relname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 
		
		// The OID of the namespace that contains this relation (pg_namespace.oid)
		addColumn("relnamespace", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
		// r = ordinary table, i = index, S = sequence, v = view, c = composite type, t = TOAST table
		addColumn("relkind", DataTypeManager.DefaultDataTypes.CHAR, t); //$NON-NLS-1$ 
		
		// 	If this is an index, the access method used (B-tree, hash, etc.)
		addColumn("relam", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
		// Number of rows in the table. This is only an estimate used by the planner. It is updated 
		// by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX
		addColumn("reltuples", DataTypeManager.DefaultDataTypes.FLOAT, t); //$NON-NLS-1$ 
		
		// Size of the on-disk representation of this table in pages (of size BLCKSZ). This is only an estimate 
		// used by the planner. It is updated by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX
		addColumn("relpages", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 

		// True if table has (or once had) rules; see pg_rewrite catalog 
		addColumn("relhasrules", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 
		
		// 	True if we generate an OID for each row of the relation
		addColumn("relhasoids", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 

		addPrimaryKey("pk_pg_class", Arrays.asList("oid"), t); //$NON-NLS-1$ //$NON-NLS-2$

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
		t.setMaterialized(true);
		return t;		
	}
	
	// additional index information
	private Table add_pg_index() throws TranslatorException {
		Table t = createView("pg_index"); //$NON-NLS-1$ 
		
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
		// 	The OID of the pg_class entry for this index
		addColumn("indexrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		// 	The OID of the pg_class entry for the table this index is for
		addColumn("indrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		// 	If true, the table was last clustered on this index
		addColumn("indisclustered", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 
		// If true, this is a unique index
		addColumn("indisunique", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 
		// If true, this index represents the primary key of the table (indisunique should always be true when this is true)
		addColumn("indisprimary", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 
		// Expression trees (in nodeToString() representation) for index attributes that are not simple 
		// column references. This is a list with one element for each zero entry in indkey. 
		// NULL if all index attributes are simple references
		addColumn("indexprs", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 
		// This is an array of indnatts values that indicate which table columns this index indexes.
		addColumn("indkey", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 
		
		addPrimaryKey("pk_pg_index", Arrays.asList("oid"), t); //$NON-NLS-1$ //$NON-NLS-2$
		
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
		t.setMaterialized(true);
		return t;		
	}

	// schemas
	private Table add_pg_namespace() throws TranslatorException {
		Table t = createView("pg_namespace"); //$NON-NLS-1$ 
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("nspname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 
		
		String transformation = "SELECT t1.OID as oid, t1.Name as nspname " + //$NON-NLS-1$
		"FROM SYS.Schemas as t1"; //$NON-NLS-1$

		t.setSelectTransformation(transformation);

		return t;		
	}
	
	// functions and procedures
	private Table add_pg_proc() throws TranslatorException {
		Table t = createView("pg_proc"); //$NON-NLS-1$ 

		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
		// Name of the function or procedure
		addColumn("proname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 

		// Function returns a set (i.e., multiple values of the specified data type)
		addColumn("proretset", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 

		// OID of 	Data type of the return value
		addColumn("prorettype", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 

		// Number of input arguments
		addColumn("pronargs", DataTypeManager.DefaultDataTypes.SHORT, t); //$NON-NLS-1$ 
		
		addColumn("proargtypes", DataTypeManager.DefaultDataTypes.OBJECT, t); //$NON-NLS-1$ 
		addColumn("proargnames", DataTypeManager.DefaultDataTypes.OBJECT, t); //$NON-NLS-1$ 
		addColumn("proargmodes", DataTypeManager.DefaultDataTypes.OBJECT, t); //$NON-NLS-1$ 
		addColumn("proallargtypes", DataTypeManager.DefaultDataTypes.OBJECT, t); //$NON-NLS-1$ 
		
		// The OID of the namespace that contains this function 
		addColumn("pronamespace", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
		addPrimaryKey("pk_pg_proc", Arrays.asList("oid"), t); //$NON-NLS-1$ //$NON-NLS-2$
		
		String transformation = "SELECT t1.OID as oid, t1.Name as proname, false as proretset, " + //$NON-NLS-1$
				"(SELECT dt.OID FROM ProcedureParams pp, DataTypes dt WHERE pp.ProcedureName = t1.Name AND pp.SchemaName = t1.SchemaName AND pp.Type = 'ResultSet' AND pp.Position = 1 AND dt.Name = pp.DataType) as prorettype, " + //$NON-NLS-1$ 
				"convert((SELECT count(*) FROM ProcedureParams pp WHERE pp.ProcedureName = t1.Name AND pp.SchemaName = t1.SchemaName ), short) as pronargs, " + //$NON-NLS-1$
				"null as proargtypes, " + //$NON-NLS-1$
				"null as proargnames, " + //$NON-NLS-1$
				"null as proargmodes, " + //$NON-NLS-1$
				"null as proallargtypes, " + //$NON-NLS-1$
				"(SELECT OID FROM SYS.Schemas WHERE Name = t1.SchemaName) as pronamespace " + //$NON-NLS-1$
				"FROM SYS.Procedures as t1"; //$NON-NLS-1$
		t.setSelectTransformation(transformation);
		t.setMaterialized(true);
		return t;		
	}

	
	// triggers
	private Table add_pg_trigger() throws TranslatorException  {
		Table t = createView("pg_trigger"); //$NON-NLS-1$ 
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("tgconstrrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("tgfoid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("tgargs", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("tgnargs", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("tgdeferrable", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 
		addColumn("tginitdeferred", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 
		addColumn("tgconstrname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 
		addColumn("tgrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
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
	private Table add_pg_type() throws TranslatorException {
		Table t = createView("pg_type"); //$NON-NLS-1$ 
		// Data type name
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		// Data type name
		addColumn("typname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 
		// The OID of the namespace that contains this type 
		addColumn("typnamespace", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		// For a fixed-size type, typlen is the number of bytes in the internal representation of the type. 
		// But for a variable-length type, typlen is negative. -1 indicates a "varlena" type (one that 
		// has a length word), -2 indicates a null-terminated C string. 
		addColumn("typlen", DataTypeManager.DefaultDataTypes.SHORT, t); //$NON-NLS-1$ 
		// 	 typtype is b for a base type, c for a composite type (e.g., a table's row type), d for a domain, 
		// e for an enum type, or p for a pseudo-type. See also typrelid and typbasetype
		addColumn("typtype", DataTypeManager.DefaultDataTypes.CHAR, t); //$NON-NLS-1$ 
		// if this is a domain (see typtype), then typbasetype identifies the type that this one is based on. 
		// Zero if this type is not a domain 
		addColumn("typbasetype", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		// Domains use typtypmod to record the typmod to be applied to their base type 
		// (-1 if base type does not use a typmod). -1 if this type is not a domain 
		addColumn("typtypmod", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
		addColumn("typrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
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
			"SELECT 23 as oid,  'integer' as typname,   (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace,  convert(4, short) as typlen,  convert('b', char) as typtype, 0 as typbasetype, -1 as typtypmod, 0 as typrelid  FROM (SELECT 1) X" + //$NON-NLS-1$
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
		Table t = createView("pg_database"); //$NON-NLS-1$ 
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("datname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 
		addColumn("encoding", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("datlastsysoid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		// this is is boolean type but the query coming in is in the form dataallowconn = 't'
		addColumn("datallowconn", DataTypeManager.DefaultDataTypes.CHAR, t); //$NON-NLS-1$ 
		addColumn("datconfig", DataTypeManager.DefaultDataTypes.OBJECT, t); //$NON-NLS-1$ 
		addColumn("datacl", DataTypeManager.DefaultDataTypes.OBJECT, t); //$NON-NLS-1$ 
		addColumn("datdba", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("dattablespace", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
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
		Table t = createView("pg_user"); //$NON-NLS-1$ 
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("usename", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 
		addColumn("usecreatedb", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 
		addColumn("usesuper", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 
		
		String transformation = "SELECT 0 as oid, " + //$NON-NLS-1$
				"null as usename, " + //$NON-NLS-1$
				"false as usecreatedb, " + //$NON-NLS-1$
				"false as usesuper "; //$NON-NLS-1$
		t.setSelectTransformation(transformation);	
		return t;
	}
}
