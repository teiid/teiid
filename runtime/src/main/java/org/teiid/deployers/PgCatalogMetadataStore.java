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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.Table.Type;
import org.teiid.translator.TranslatorException;

public class PgCatalogMetadataStore extends MetadataFactory {
	
    public static final int PG_TYPE_OIDVECTOR = 30;
    public static final int PG_TYPE_OIDARRAY = 1028;
    public static final int PG_TYPE_CHARARRAY = 1002;
    public static final int PG_TYPE_TEXTARRAY = 1009;

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
		add_matpg_relatt();
		add_matpg_datatype();
		addHasFunctionPrivilage();
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

		addColumn("adrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		addColumn("adnum", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
		addColumn("adbin", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
		addColumn("adsrc", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
		
		String transformation = "SELECT null as oid, null as adsrc, null as adrelid, null as adnum from sys.tables where 1 = 0"; //$NON-NLS-1$
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
				"(CASE WHEN (t1.DataType = 'bigdecimal' OR t1.DataType = 'biginteger' OR t1.DataType = 'float' OR t1.DataType='double') THEN (4+(65536*t1.Precision)+t1.Scale) " + //$NON-NLS-1$
				"ELSE (4+t1.Length) END) as atttypmod, " + //$NON-NLS-1$
				"CASE WHEN (t1.NullType = 'No Nulls') THEN true ELSE false END as attnotnull, " + //$NON-NLS-1$
				"false as attisdropped, " + //$NON-NLS-1$
				"false as atthasdef " + //$NON-NLS-1$
				"FROM SYS.Columns as t1 LEFT OUTER JOIN " + //$NON-NLS-1$
				"SYS.Tables st ON (st.Name = t1.TableName AND st.SchemaName = t1.SchemaName) LEFT OUTER JOIN " + //$NON-NLS-1$
				"pg_catalog.matpg_datatype pt ON t1.DataType = pt.Name";//$NON-NLS-1$
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
		addColumn("relhasoids", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$ 

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
				"'0' as indkey " + //$NON-NLS-1$
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
		
		Column c = addColumn("proargtypes", DataTypeManager.DefaultDataTypes.OBJECT, t); //$NON-NLS-1$
		c.setProperty("pg_type:oid", String.valueOf(PG_TYPE_OIDVECTOR)); //$NON-NLS-1$
		
		c = addColumn("proargnames", DataTypeManager.DefaultDataTypes.OBJECT, t); //$NON-NLS-1$
		c.setProperty("pg_type:oid", String.valueOf(PG_TYPE_TEXTARRAY)); //$NON-NLS-1$
		
		c = addColumn("proargmodes", DataTypeManager.DefaultDataTypes.OBJECT, t); //$NON-NLS-1$
		c.setProperty("pg_type:oid", String.valueOf(PG_TYPE_CHARARRAY)); //$NON-NLS-1$
		
		c = addColumn("proallargtypes", DataTypeManager.DefaultDataTypes.OBJECT, t); //$NON-NLS-1$
		c.setProperty("pg_type:oid", String.valueOf(PG_TYPE_OIDARRAY)); //$NON-NLS-1$
		
		// The OID of the namespace that contains this function 
		addColumn("pronamespace", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$ 
		
		addPrimaryKey("pk_pg_proc", Arrays.asList("oid"), t); //$NON-NLS-1$ //$NON-NLS-2$
		
		
		String transformation = "SELECT t1.OID as oid, t1.Name as proname, (SELECT (CASE WHEN count(pp.Type)>0 THEN true else false END) as x FROM ProcedureParams pp WHERE pp.ProcedureName = t1.Name AND pp.SchemaName = t1.SchemaName and pp.Type='ResultSet') as proretset, " + //$NON-NLS-1$
		"CASE WHEN (SELECT count(dt.oid) FROM ProcedureParams pp, matpg_datatype dt WHERE pp.ProcedureName = t1.Name AND pp.SchemaName = t1.SchemaName AND pp.Type IN ('ReturnValue', 'ResultSet') AND dt.Name = pp.DataType) IS NULL THEN (select oid from pg_type WHERE typname = 'void') WHEN (SELECT count(dt.oid) FROM ProcedureParams pp, matpg_datatype dt WHERE pp.ProcedureName = t1.Name AND pp.SchemaName = t1.SchemaName AND pp.Type = 'ResultSet' AND dt.Name = pp.DataType) IS NOT NULL THEN (select oid from pg_type WHERE typname = 'record') ELSE (SELECT dt.oid FROM ProcedureParams pp, matpg_datatype dt WHERE pp.ProcedureName = t1.Name AND pp.SchemaName = t1.SchemaName AND pp.Type = 'ReturnValue' AND dt.Name = pp.DataType) END as prorettype,  " + //$NON-NLS-1$
		"convert((SELECT count(*) FROM ProcedureParams pp WHERE pp.ProcedureName = t1.Name AND pp.SchemaName = t1.SchemaName AND pp.Type IN ('In', 'InOut')), short) as pronargs, " + //$NON-NLS-1$
		"(select "+arrayAgg("y.oid","y.type, y.position" )+" FROM ("+paramTable("'ResultSet','ReturnValue', 'Out'")+") as y) as proargtypes, " +//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
		"(select "+arrayAgg("y.name", "y.type, y.position")+" FROM (SELECT pp.Name as name, pp.position as position, pp.Type as type FROM ProcedureParams pp WHERE pp.ProcedureName = t1.Name AND pp.SchemaName = t1.SchemaName AND pp.Type NOT IN ('ReturnValue' )) as y) as proargnames, " + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		"(select case WHEN count(distinct(y.type)) = 1 THEN null ELSE "+arrayAgg("CASE WHEN (y.type ='In') THEN 'i' WHEN (y.type = 'Out') THEN 'o' WHEN (y.type = 'InOut') THEN 'b' WHEN (y.type = 'ResultSet') THEN 't' END", "y.type,y.position")+" END FROM (SELECT pp.Type as type, pp.Position as position FROM ProcedureParams pp WHERE pp.ProcedureName = t1.Name AND pp.SchemaName = t1.SchemaName AND pp.Type NOT IN ('ReturnValue')) as y) as proargmodes, " + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		"(select case WHEN count(distinct(y.oid)) = 1 THEN null ELSE "+arrayAgg("y.oid", "y.type, y.position")+" END FROM ("+paramTable("'ReturnValue'")+") as y) as proallargtypes, " +  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
		"(SELECT OID FROM SYS.Schemas WHERE Name = t1.SchemaName) as pronamespace " + //$NON-NLS-1$
		"FROM SYS.Procedures as t1";//$NON-NLS-1$			
		
		t.setSelectTransformation(transformation);
		t.setMaterialized(true);
		return t;		
	}
	
	private String paramTable(String notIn) {
		return "SELECT case when pp.Type <> 'ResultSet' AND pp.DataType = 'object' then 2283 else dt.oid end as oid, pp.Position as position, pp.Type as type FROM ProcedureParams pp LEFT JOIN matpg_datatype dt ON pp.DataType=dt.Name " + //$NON-NLS-1$
				"WHERE pp.ProcedureName = t1.Name AND pp.SchemaName = t1.SchemaName AND pp.Type NOT IN ("+notIn+")"; //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	
	private String arrayAgg(String select, String orderby) {
		return "array_agg("+select+" ORDER BY "+orderby+")";  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
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
		addColumn("typelem", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
		String transformation =
			"select oid, typname, (SELECT OID FROM SYS.Schemas where Name = 'SYS') as typnamespace, typlen, typtype, typbasetype, typtypmod, typrelid, typelem from texttable('" + //$NON-NLS-1$
			"16,boolean,1,b,0,-1,0,0\n" + //$NON-NLS-1$
			"1043,string,-1,b,0,-1,0,0\n" + //$NON-NLS-1$
			"25,text,-1,b,0,-1,0,0\n" + //$NON-NLS-1$
			"1042,char,1,b,0,-1,0,0\n" + //$NON-NLS-1$
			"21,short,2,b,0,-1,0,0\n" + //$NON-NLS-1$
			"20,long,8,b,0,-1,0,0\n" + //$NON-NLS-1$
			"23,integer,4,b,0,-1,0,0\n" + //$NON-NLS-1$
			"26,oid,4,b,0,-1,0,0\n" + //$NON-NLS-1$
			"700,float,4,b,0,-1,0,0\n" + //$NON-NLS-1$
			"701,double,8,b,0,-1,0,0\n" + //$NON-NLS-1$
			"705,unknown,-2,b,0,-1,0,0\n" + //$NON-NLS-1$
			"1082,date,4,b,0,-1,0,0\n" + //$NON-NLS-1$
			"1083,datetime,8,b,0,-1,0,0\n" + //$NON-NLS-1$
			"1114,timestamp,8,b,0,-1,0,0\n" + //$NON-NLS-1$
			"1700,decimal,-1,b,0,-1,0,0\n" + //$NON-NLS-1$
			"142,xml,-1,b,0,-1,0,0\n" + //$NON-NLS-1$
			"14939,lo,-1,b,0,-1,0,0\n" + //$NON-NLS-1$
			"2278,void,4,p,0,-1,0,0\n" + //$NON-NLS-1$
			"2249,record,-1,p,0,-1,0,0\n" + //$NON-NLS-1$
			"30,oidvector,-1,b,0,-1,0,26\n" + //$NON-NLS-1$
			"1000,_bool,-1,b,0,-1,0,16\n" + //$NON-NLS-1$
			"1002,_char,-1,b,0,-1,0,18\n" + //$NON-NLS-1$
			"1005,_int2,-1,b,0,-1,0,21\n" + //$NON-NLS-1$
			"1007,_int4,-1,b,0,-1,0,23\n" + //$NON-NLS-1$
			"1009,_text,-1,b,0,-1,0,25\n" + //$NON-NLS-1$
			"1028,_oid,-1,b,0,-1,0,26\n" + //$NON-NLS-1$
			"1014,_bpchar,-1,b,0,-1,0,1042\n" + //$NON-NLS-1$
			"1015,_varchar,-1,b,0,-1,0,1043\n" + //$NON-NLS-1$
			"1016,_int8,-1,b,0,-1,0,20\n" + //$NON-NLS-1$
			"1021,_float4,-1,b,0,-1,0,700\n" + //$NON-NLS-1$
			"1022,_float8,-1,b,0,-1,0,701\n" + //$NON-NLS-1$
			"1115,_timestamp,-1,b,0,-1,0,1114\n" + //$NON-NLS-1$
			"1182,_date,-1,b,0,-1,0,1082\n" + //$NON-NLS-1$
			"1183,_time,-1,b,0,-1,0,1083\n" + //$NON-NLS-1$
			"2287,_record,-1,b,0,-1,0,2249\n" + //$NON-NLS-1$
			"2283,anyelement,4,p,0,-1,0,0" + //$NON-NLS-1$
			"' columns oid integer, typname string, typlen short, typtype char, typbasetype integer, typtypmod integer, typrelid integer, typelem integer) AS t"; //$NON-NLS-1$
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
	
	private Table add_matpg_relatt() throws TranslatorException  {
		Table t = createView("matpg_relatt"); //$NON-NLS-1$ 
		addColumn("attrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
		addColumn("attnum", DataTypeManager.DefaultDataTypes.SHORT, t); //$NON-NLS-1$ 
		addColumn("attname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 
		addColumn("relname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
		addColumn("nspname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
		addColumn("autoinc", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$
		addColumn("typoid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
		
		addPrimaryKey("pk_matpg_relatt_names", Arrays.asList("attname", "relname", "nspname"), t); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$  
		addIndex("idx_matpg_relatt_ids", true, Arrays.asList("attrelid", "attnum"), t); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
		String transformation = "select pg_class.oid as attrelid, attnum, attname, relname, nspname, IsAutoIncremented as autoinc, cast((select p.value from SYS.Properties p where p.name = 'pg_type:oid' and p.uid = SYS.Columns.uid) as integer) as typoid " + //$NON-NLS-1$
				"from pg_attribute, pg_class, pg_namespace, SYS.Columns " + //$NON-NLS-1$
				"where pg_attribute.attrelid = pg_class.oid and pg_namespace.oid = relnamespace" + //$NON-NLS-1$
				" and SchemaName = nspname and TableName = relname and Name = attname";  //$NON-NLS-1$
		t.setSelectTransformation(transformation);
		t.setMaterialized(true);
		return t;
	}
	
	private Table add_matpg_datatype() throws TranslatorException  {
		Table t = createView("matpg_datatype"); //$NON-NLS-1$ 
		addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
		addColumn("typname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 
		addColumn("name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$ 
		addColumn("uid", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
		addColumn("typlen", DataTypeManager.DefaultDataTypes.SHORT, t); //$NON-NLS-1$
		
		addPrimaryKey("matpg_datatype_names", Arrays.asList("oid", "name"), t); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
		addIndex("matpg_datatype_ids", true, Arrays.asList("typname", "oid"), t); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
		String transformation = "select pt.oid as oid, pt.typname as typname, t.Name name, t.UID, pt.typlen from pg_catalog.pg_type pt JOIN (select (CASE "+//$NON-NLS-1$
		"WHEN (Name = 'clob' OR Name = 'blob') THEN 'lo' " +//$NON-NLS-1$
		"WHEN (Name = 'byte' ) THEN 'short' " +//$NON-NLS-1$
		"WHEN (Name = 'time' ) THEN 'datetime' " + //$NON-NLS-1$
		"WHEN (Name = 'biginteger' ) THEN 'decimal' " +//$NON-NLS-1$
		"WHEN (Name = 'bigdecimal' ) THEN 'decimal' " +//$NON-NLS-1$
		"WHEN (Name = 'object' ) THEN 'unknown' " +//$NON-NLS-1$
		"ELSE Name END) as pg_name, Name, UID from SYS.DataTypes) as t ON t.pg_name = pt.typname";  //$NON-NLS-1$
		t.setSelectTransformation(transformation);
		t.setMaterialized(true);
		return t;
	}	
	
	private FunctionMethod addHasFunctionPrivilage() throws TranslatorException  {
		FunctionMethod func = addFunction("has_function_privilege"); //$NON-NLS-1$
		
		ArrayList<FunctionParameter> inParams = new ArrayList<FunctionParameter>();
		inParams.add(new FunctionParameter("oid", DataTypeManager.DefaultDataTypes.INTEGER, ""));//$NON-NLS-1$ //$NON-NLS-2$
		inParams.add(new FunctionParameter("permission", DataTypeManager.DefaultDataTypes.STRING, "")); //$NON-NLS-1$ //$NON-NLS-2$
		
		func.setInputParameters(inParams);
		func.setOutputParameter(new FunctionParameter("result", DataTypeManager.DefaultDataTypes.BOOLEAN, ""));  //$NON-NLS-1$ //$NON-NLS-2$
		
		func.setInvocationClass(ReturnTrue.class.getName());
		func.setInvocationMethod("result"); //$NON-NLS-1$
		func.setPushdown(PushDown.CANNOT_PUSHDOWN);
		return func;
	}
	
	public static class ReturnTrue{
		public static boolean result(@SuppressWarnings("unused")int oid, @SuppressWarnings("unused") String permission) {
			return true;
		}
	}
}
