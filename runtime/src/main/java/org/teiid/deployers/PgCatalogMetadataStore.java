/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.deployers;

import static org.teiid.odbc.PGUtil.PG_TYPE_CHARARRAY;
import static org.teiid.odbc.PGUtil.PG_TYPE_INT2VECTOR;
import static org.teiid.odbc.PGUtil.PG_TYPE_OIDARRAY;
import static org.teiid.odbc.PGUtil.PG_TYPE_OIDVECTOR;
import static org.teiid.odbc.PGUtil.PG_TYPE_TEXTARRAY;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.teiid.CommandContext;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.GeographyType;
import org.teiid.core.types.GeometryType;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.StringUtil;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.language.SQLConstants;
import org.teiid.metadata.Column;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.Type;
import org.teiid.odbc.ODBCServerRemoteImpl;
import org.teiid.query.function.GeometryFunctionMethods;
import org.teiid.query.metadata.MaterializationMetadataRepository;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.SQLParserUtil;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;
import org.teiid.transport.PgBackendProtocol;

public class PgCatalogMetadataStore extends MetadataFactory {
    private static final long serialVersionUID = 2158418324376966987L;

    public static final String POSTGIS_LIB_VERSION = "2.0.0 USE_GEOS=0 USE_PROJ=1 USE_STATS=0"; //$NON-NLS-1$

    public static final String POSTGRESQL_VERSION = PropertiesUtils.getHierarchicalProperty("org.teiid.pgVersion", "PostgreSQL 8.2"); //$NON-NLS-1$ //$NON-NLS-2$

    public static final String TYPMOD = "(CASE WHEN (t1.DataType = 'bigdecimal' OR t1.DataType = 'biginteger') THEN 4+(65536*(case when (t1.Precision>32767) then 32767 else t1.Precision end)+(case when (t1.Scale>32767) then 32767 else t1.Scale end)) " + //$NON-NLS-1$
                "WHEN (t1.DataType = 'string' OR t1.DataType = 'char') THEN (CASE WHEN (t1.Length <= 2147483643) THEN 4+ t1.Length ELSE 2147483647 END) ELSE -1 END)"; //$NON-NLS-1$

    public PgCatalogMetadataStore(String modelName, Map<String, Datatype> dataTypes) {
        super(modelName, 1, modelName, dataTypes, new Properties(), null);
        add_pg_namespace();
        add_pg_class();
        add_pg_tables();
        add_pg_attribute();
        add_pg_type();
        add_pg_index();
        add_pg_am();
        add_pg_proc();
        add_pg_trigger();
        add_pg_attrdef();
        add_pg_database();
        add_pg_user();
        add_matpg_datatype();
        add_pg_description();
        add_pg_prepared_xacts();
        add_pg_inherits();
        add_pg_stats();
        add_pg_constraint();
        add_pg_tablespace();
        add_pg_roles();
        add_pg_rewrite();

        //limited emulation of information_schema
        add_infoSchemaTables();
        add_infoSchemaViews();
        add_infoSchemaColumns();
        add_infoSchemaReferentialConstraints();
        add_infoSchemaKeyColumnUsage();
        add_infoSchemaTableConstraints();

        addFunction("regClass", "regclass").setNullOnNull(true); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("encode", "encode").setPushdown(PushDown.CAN_PUSHDOWN); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("objDescription", "obj_description"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("objDescription2", "obj_description"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("hasSchemaPrivilege", "has_schema_privilege").setNullOnNull(true); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("hasTablePrivilege", "has_table_privilege").setNullOnNull(true); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("formatType", "format_type"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("currentSchema", "current_schema"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("getUserById", "pg_get_userbyid"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("colDescription", "col_description"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("pgHasRole", "pg_has_role"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("asBinary2", "ST_asBinary"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("asBinary3", "ST_asBinary"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("postgisLibVersion", "public.PostGIS_Lib_Version"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("postgisGeosVersion", "public.postgis_geos_version"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("postgisProjVersion", "public.postgis_proj_version"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("postgisVersion", "public.postgis_version"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("hasPerm", "has_function_privilege"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("getExpr2", "pg_get_expr"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("getExpr3", "pg_get_expr"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("pg_table_is_visible", "pg_table_is_visible"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("pg_get_constraintdef", "pg_get_constraintdef"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("pg_type_is_visible", "pg_type_is_visible"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("pg_encoding_to_char", "pg_encoding_to_char"); //$NON-NLS-1$ //$NON-NLS-2$
        FunctionMethod func = addFunction("asPGVector", "asPGVector"); //$NON-NLS-1$ //$NON-NLS-2$
        func.setProperty(ResolverVisitor.TEIID_PASS_THROUGH_TYPE, Boolean.TRUE.toString());
        addFunction("getOid", "getOid").setNullOnNull(true);; //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("version", "version"); //$NON-NLS-1$ //$NON-NLS-2$
        func = addFunction("pg_client_encoding", "pg_client_encoding"); //$NON-NLS-1$ //$NON-NLS-2$
        func.setDeterminism(Determinism.COMMAND_DETERMINISTIC);
        addFunction("current_schemas", "current_schemas"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("pg_get_indexdef", "pg_get_indexdef"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("array_to_string", "array_to_string").setNullOnNull(true); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("pg_total_relation_size", "pg_total_relation_size"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("pg_relation_size", "pg_relation_size"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("pg_stat_get_numscans", "pg_stat_get_numscans"); //$NON-NLS-1$ //$NON-NLS-2$
        addFunction("pg_get_ruledef", "pg_get_ruledef"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    private Table add_pg_rewrite() {
        Table t = createView("pg_rewrite"); //$NON-NLS-1$

        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("ev_class", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("rulename", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        String transformation = "SELECT null, null, null from SYS.Tables WHERE 1=2"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);

        return t;
    }

    private Table add_pg_tablespace() {
        Table t = createView("pg_tablespace"); //$NON-NLS-1$

        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("spcname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("spcowner", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        //actually a struct array
        addColumn("spcacl", "string[]", t); //$NON-NLS-1$ //$NON-NLS-2$
        addColumn("spcoptions", "string[]", t); //$NON-NLS-1$ //$NON-NLS-2$

        String transformation = "SELECT null, null, null, null, null from SYS.Tables WHERE 1=2"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);

        return t;
    }

    private Table add_pg_roles() {
        Table t = createView("pg_roles"); //$NON-NLS-1$

        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("rolname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        String transformation = "SELECT null, null from SYS.Tables WHERE 1=2"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);

        return t;
    }

    private Table add_pg_constraint() {
        Table t = createView("pg_constraint"); //$NON-NLS-1$

        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("conname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("connamespace", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("contype", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("condeferrable", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$
        addColumn("condeferred", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$
        addColumn("consrc", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("conrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("confrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("conkey", DataTypeManager.getDataTypeName(DataTypeManager.getArrayType(DataTypeManager.DefaultDataClasses.SHORT)), t); //$NON-NLS-1$

        String transformation = "SELECT pg_catalog.getOid(UID) as oid, name as conname, pg_catalog.getOid(SchemaUID), " //$NON-NLS-1$
                + "lower(left(Type, 1)) as contype, false as condeferrable, " //$NON-NLS-1$
                + "false as condeferred, null as consrc, " //$NON-NLS-1$
                + "pg_catalog.getOid(TableUID) as conrelid, pg_catalog.getOid(RefTableUID) as confrelid, " //$NON-NLS-1$
                + "ColPositions as conkey " + //$NON-NLS-1$
                "FROM Sys.Keys WHERE Type in ('Primary', 'Unique', 'Foreign')"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);
        return t;

    }

    private void add_pg_prepared_xacts() {
        Table t = createView("pg_prepared_xacts"); //$NON-NLS-1$
        //xid
        addColumn("transaction", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("gid", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("owner", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("database", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        String transformation = "SELECT null, null, null, null from SYS.Tables WHERE 1=2"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);
    }

    private void add_pg_inherits() {
        Table t = createView("pg_inherits"); //$NON-NLS-1$
        addColumn("inhrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("inhparent", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("inhseqno", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        String transformation = "SELECT null, null, null from SYS.Tables WHERE 1=2"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);
    }

    private void add_pg_stats() {
        Table t = createView("pg_stats"); //$NON-NLS-1$
        addColumn("schemaname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("tablename", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("attname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        String transformation = "SELECT null, null, null from SYS.Tables WHERE 1=2"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);
    }

    private Table createView(String name) {
        Table t = addTable(name);
        t.setSystem(true);
        t.setSupportsUpdate(false);
        t.setVirtual(true);
        t.setTableType(Type.Table);
        return t;
    }

    //TODO: implement using the oid index on the metadata
    private Table add_pg_description() {
        Table t = createView("pg_description"); //$NON-NLS-1$
        addColumn("objoid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("classoid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("objsubid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("description", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        String transformation = "SELECT 0, null, null, null"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);

        return t;
    }

    //index access methods
    private Table add_pg_am() {
        Table t = createView("pg_am"); //$NON-NLS-1$
        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("amname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        String transformation = "SELECT 0 as oid, 'btree' as amname"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);

        return t;
    }

    // column default values
    private Table add_pg_attrdef() {
        Table t = createView("pg_attrdef"); //$NON-NLS-1$

        addColumn("adrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("adnum", DataTypeManager.DefaultDataTypes.SHORT, t); //$NON-NLS-1$
        addColumn("adbin", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("adsrc", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        String transformation = "SELECT pg_catalog.getOid(t1.tableuid) as adrelid, convert(t1.Position, short) as adnum, " + //$NON-NLS-1$
                "case when t1.IsAutoIncremented then 'nextval(' else t1.DefaultValue end as adbin, " + //$NON-NLS-1$
                "case when t1.IsAutoIncremented then 'nextval(' else t1.DefaultValue end as adsrc " + //$NON-NLS-1$
                "FROM SYS.Columns as t1"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);
        return t;
    }

    /**
     * table columns ("attributes")
     * see also {@link ODBCServerRemoteImpl} getPGColInfo for the mod calculation
     */
    private Table add_pg_attribute() {
        Table t = createView("pg_attribute"); //$NON-NLS-1$

        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        // OID, The table this column belongs to
        addColumn("attrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        //     The column name
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

        String transformation = "SELECT pg_catalog.getOid(t1.uid) as oid, " + //$NON-NLS-1$
                "pg_catalog.getOid(t1.TableUID) as attrelid, " + //$NON-NLS-1$
                "t1.Name as attname, " + //$NON-NLS-1$
                "pt.oid as atttypid," + //$NON-NLS-1$
                "pt.typlen as attlen, " + //$NON-NLS-1$
                "convert(t1.Position, short) as attnum, " + //$NON-NLS-1$
                TYPMOD +" as atttypmod, " + //$NON-NLS-1$
                "CASE WHEN (t1.NullType = 'No Nulls') THEN true ELSE false END as attnotnull, " + //$NON-NLS-1$
                "false as attisdropped, " + //$NON-NLS-1$
                "false as atthasdef " + //$NON-NLS-1$
                "FROM SYS.Columns as t1 LEFT OUTER JOIN " + //$NON-NLS-1$
                "pg_catalog.matpg_datatype pt ON t1.DataType = pt.Name " +//$NON-NLS-1$
                "UNION ALL SELECT pg_catalog.getOid(kc.uid) + kc.position as oid, " + //$NON-NLS-1$
                "pg_catalog.getOid(kc.uid) as attrelid, " + //$NON-NLS-1$
                "t1.Name as attname, " + //$NON-NLS-1$
                "pt.oid as atttypid," + //$NON-NLS-1$
                "pt.typlen as attlen, " + //$NON-NLS-1$
                "convert(kc.Position, short) as attnum, " + //$NON-NLS-1$
                TYPMOD +" as atttypmod, " + //$NON-NLS-1$
                "CASE WHEN (t1.NullType = 'No Nulls') THEN true ELSE false END as attnotnull, " + //$NON-NLS-1$
                "false as attisdropped, " + //$NON-NLS-1$
                "false as atthasdef " + //$NON-NLS-1$
                "FROM (SYS.KeyColumns as kc INNER JOIN SYS.Columns as t1 ON kc.SchemaName = t1.SchemaName AND kc.TableName = t1.TableName AND kc.Name = t1.Name) LEFT OUTER JOIN " + //$NON-NLS-1$
                "pg_catalog.matpg_datatype pt ON t1.DataType = pt.Name WHERE kc.keytype in ('Primary', 'Unique', 'Index')"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);
        return t;
    }

    private Table add_pg_tables() {
        Table t = createView("pg_tables"); //$NON-NLS-1$
        addColumn("schemaname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("tablename", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("tableowner", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("tablespace", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("hasindexes", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$
        addColumn("hasrules", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$
        //currently false - see pg_triggers
        addColumn("hastriggers", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$

        t.setSelectTransformation("select schemaname, name, 'teiid', null, " //$NON-NLS-1$
                + "exists (select 1 from SYS.Keys WHERE type = 'Index' and TableUID = UID), false, false from SYS.Tables WHERE isPhysical"); //$NON-NLS-1$

        return t;
    }

    // tables, indexes, sequences ("relations")
    private Table add_pg_class() {
        Table t = createView("pg_class"); //$NON-NLS-1$

        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        // Name of the table, index, view, etc
        addColumn("relname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        // The OID of the namespace that contains this relation (pg_namespace.oid)
        addColumn("relnamespace", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        // r = ordinary table, i = index, S = sequence, v = view, c = composite type, t = TOAST table
        addColumn("relkind", DataTypeManager.DefaultDataTypes.CHAR, t); //$NON-NLS-1$

        addColumn("relowner", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        //     If this is an index, the access method used (B-tree, hash, etc.)
        addColumn("relam", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        addColumn("reltablespace", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        // Number of rows in the table. This is only an estimate used by the planner. It is updated
        // by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX
        addColumn("reltuples", DataTypeManager.DefaultDataTypes.FLOAT, t); //$NON-NLS-1$

        // Size of the on-disk representation of this table in pages (of size BLCKSZ). This is only an estimate
        // used by the planner. It is updated by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX
        addColumn("relpages", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        // True if table has (or once had) rules; see pg_rewrite catalog
        addColumn("relhasrules", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$

        //     True if we generate an OID for each row of the relation
        addColumn("relhasoids", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$

        //additional column not present in pg metadata - for column metadata query
        addColumn("relnspname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        addColumn("reloptions", DataTypeManager.getDataTypeName(DataTypeManager.getArrayType(DataTypeManager.DefaultDataClasses.STRING)), t); //$NON-NLS-1$

        addColumn("relacl", "object[]", t); //$NON-NLS-1$ //$NON-NLS-2$

        addPrimaryKey("pk_pg_class", Arrays.asList("oid"), t); //$NON-NLS-1$ //$NON-NLS-2$

        String transformation = "SELECT pg_catalog.getOid(t1.uid) as oid, t1.name as relname, " +  //$NON-NLS-1$
                "pg_catalog.getOid(t1.SchemaUID) as relnamespace, " + //$NON-NLS-1$
                //TODO: starting with pg 9.1 f is used for foreign, once we update our reported version
                //we can use that
                "convert((CASE t1.isPhysical WHEN true THEN 'r' ELSE 'v' END), char) as relkind," + //$NON-NLS-1$
                "0 as relowner, " + //$NON-NLS-1$
                "0 as relam, " + //$NON-NLS-1$
                "0 as reltablespace, " + //$NON-NLS-1$
                "convert(0, float) as reltuples, " + //$NON-NLS-1$
                "0 as relpages, " + //$NON-NLS-1$
                "false as relhasrules, " + //$NON-NLS-1$
                "false as relhasoids, " + //$NON-NLS-1$
                "t1.SchemaName as relnspname, " + //$NON-NLS-1$
                "null as reloptions, null as relacl " + //$NON-NLS-1$
                "FROM SYS.Tables t1 UNION ALL SELECT pg_catalog.getOid(t1.uid) as oid, t1.name as relname, " +  //$NON-NLS-1$
                "pg_catalog.getOid(uid) as relnamespace, " + //$NON-NLS-1$
                "convert('i', char) as relkind," + //$NON-NLS-1$
                "0 as relowner, " + //$NON-NLS-1$
                "0 as relam, " + //$NON-NLS-1$
                "0 as reltablespace, " + //$NON-NLS-1$
                "convert(0, float) as reltuples, " + //$NON-NLS-1$
                "0 as relpages, " + //$NON-NLS-1$
                "false as relhasrules, " + //$NON-NLS-1$
                "false as relhasoids, " + //$NON-NLS-1$
                "t1.SchemaName as relnspname, " + //$NON-NLS-1$
                "null as reloptions, null as relacl " + //$NON-NLS-1$
                "FROM SYS.Keys t1 WHERE t1.type in ('Primary', 'Unique', 'Index')"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);
        return t;
    }

    // additional index information
    private Table add_pg_index() {
        Table t = createView("pg_index"); //$NON-NLS-1$
        //teiid specific - there's no oid in pg
        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        //     The OID of the pg_class entry for this index
        addColumn("indexrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        //     The OID of the pg_class entry for the table this index is for
        addColumn("indrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("indnatts", DataTypeManager.DefaultDataTypes.SHORT, t); //$NON-NLS-1$
        //     If true, the table was last clustered on this index
        addColumn("indisclustered", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$
        // If true, this is a unique index
        addColumn("indisunique", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$
        // If true, this index represents the primary key of the table (indisunique should always be true when this is true)
        addColumn("indisprimary", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$

        // This is an array of indnatts values that indicate which table columns this index indexes.
        Column c = addColumn("indkey", DataTypeManager.getDataTypeName(DataTypeManager.getArrayType(DataTypeManager.DefaultDataClasses.SHORT)), t); //$NON-NLS-1$
        c.setProperty("pg_type:oid", String.valueOf(PG_TYPE_INT2VECTOR)); //$NON-NLS-1$

        // Expression trees (in nodeToString() representation) for index attributes that are not simple
        // column references. This is a list with one element for each zero entry in indkey.
        // NULL if all index attributes are simple references
        addColumn("indexprs", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("indpred", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        //teiid specific column for pg_index_def
        addColumn("indkey_names", DataTypeManager.getDataTypeName(DataTypeManager.getArrayType(DataTypeManager.DefaultDataClasses.STRING)), t); //$NON-NLS-1$

        addPrimaryKey("pk_pg_index", Arrays.asList("indexrelid"), t); //$NON-NLS-1$ //$NON-NLS-2$

        String transformation = "SELECT pg_catalog.getOid(t1.uid) as oid, " + //$NON-NLS-1$
                "pg_catalog.getOid(t1.uid) as indexrelid, " + //$NON-NLS-1$
                "pg_catalog.getOid(t1.TableUID) as indrelid, " + //$NON-NLS-1$
                "cast(ARRAY_LENGTH(ColPositions) as short) as indnatts, " + //$NON-NLS-1$
                "false indisclustered, " + //$NON-NLS-1$
                "(CASE WHEN t1.Type in ('Unique', 'Primary') THEN true ELSE false END) as indisunique, " + //$NON-NLS-1$
                "(CASE t1.Type WHEN 'Primary' THEN true ELSE false END) as indisprimary, " + //$NON-NLS-1$
                "asPGVector(ColPositions) as indkey, " + //$NON-NLS-1$
                "null as indexprs, null as indpred, " + //$NON-NLS-1$
                "ColNames as indkey_names " + //$NON-NLS-1$
                "FROM Sys.Keys as t1"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);
        return t;
    }

    // schemas
    private Table add_pg_namespace() {
        Table t = createView("pg_namespace"); //$NON-NLS-1$
        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("nspname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("nspowner", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        String transformation = "SELECT pg_catalog.getOid(uid) as oid, t1.Name as nspname, 0 as nspowner " + //$NON-NLS-1$
        "FROM SYS.Schemas as t1"; //$NON-NLS-1$

        t.setSelectTransformation(transformation);

        return t;
    }

    // functions and procedures
    private Table add_pg_proc() {
        Table t = createView("pg_proc"); //$NON-NLS-1$

        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        // Name of the function or procedure
        addColumn("proname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        // Function returns a set (i.e., multiple values of the specified data type)
        addColumn("proretset", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$

        // OID of     Data type of the return value
        addColumn("prorettype", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        // Number of input arguments
        addColumn("pronargs", DataTypeManager.DefaultDataTypes.SHORT, t); //$NON-NLS-1$

        Column c = addColumn("proargtypes", DataTypeManager.getDataTypeName(DataTypeManager.getArrayType(DataTypeManager.DefaultDataClasses.INTEGER)), t); //$NON-NLS-1$
        c.setProperty("pg_type:oid", String.valueOf(PG_TYPE_OIDVECTOR)); //$NON-NLS-1$

        c = addColumn("proargnames", DataTypeManager.getDataTypeName(DataTypeManager.getArrayType(DataTypeManager.DefaultDataClasses.STRING)), t); //$NON-NLS-1$
        c.setProperty("pg_type:oid", String.valueOf(PG_TYPE_TEXTARRAY)); //$NON-NLS-1$

        c = addColumn("proargmodes", DataTypeManager.getDataTypeName(DataTypeManager.getArrayType(DataTypeManager.DefaultDataClasses.CHAR)), t); //$NON-NLS-1$
        c.setProperty("pg_type:oid", String.valueOf(PG_TYPE_CHARARRAY)); //$NON-NLS-1$

        c = addColumn("proallargtypes", DataTypeManager.getDataTypeName(DataTypeManager.getArrayType(DataTypeManager.DefaultDataClasses.INTEGER)), t); //$NON-NLS-1$
        c.setProperty("pg_type:oid", String.valueOf(PG_TYPE_OIDARRAY)); //$NON-NLS-1$

        // The OID of the namespace that contains this function
        addColumn("pronamespace", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        addPrimaryKey("pk_pg_proc", Arrays.asList("oid"), t); //$NON-NLS-1$ //$NON-NLS-2$

        String transformation = procQuery("Procedure") //$NON-NLS-1$
                //add in Functions - restricting to just pg_catalog functions initial
                //TODO: systemsource sys functions don't yet have a uid/oid
                + " UNION ALL " + procQuery("Function") + " WHERE t1.SchemaName = 'pg_catalog'";    //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        //add in internal conversion functions that don't actually exist
        //some of the recv names don't match, but only oidvectorrecv and array_recv are called out specifically by the npgsql driver
        transformation += " union all SELECT typreceive as oid, case when kind = 'a' then 'array_recv' else kind end as proname, " //$NON-NLS-1$
                + "false as proretset, oid as prorettype, " //$NON-NLS-1$
                + "cast(case when kind = 'a' then 3 else 1 end as short) as pronargs," //$NON-NLS-1$
                + "case when kind = 'a' then (2281, 26, 23) else (2281,) end as proargtypes," //$NON-NLS-1$
                + "null as proargnames, null as proargmodes, null as proallargtypes," //$NON-NLS-1$
                + "(select oid from pg_catalog.pg_namespace where nspname = 'pg_catalog') as pronamespace FROM" //$NON-NLS-1$
                + "(SELECT typreceive, case when typelem <> 0 and typname like '\\__%' escape '\\' then 'a' else typname || 'recv' end as kind, oid " //$NON-NLS-1$
                + "from pg_catalog.pg_type where typname not in ('internal', 'anyelement')) v"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);
        return t;
    }

    private String procQuery(String type) {
        return "SELECT pg_catalog.getOid(t1.uid) as oid, t1.Name as proname, (SELECT (CASE WHEN count(pp.Type)>0 THEN true else false END) as x FROM SYS." + type + "Params pp WHERE pp." + type + "Name = t1.Name AND pp.SchemaName = t1.SchemaName AND pp." + type + "UID = t1.UID and pp.Type='ResultSet') as proretset, " + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        "CASE WHEN (SELECT count(dt.oid) FROM SYS." + type + "Params pp, pg_catalog.matpg_datatype dt WHERE pp." + type + "Name = t1.Name AND pp.SchemaName = t1.SchemaName AND pp." + type + "UID = t1.UID AND pp.Type IN ('ReturnValue', 'ResultSet') AND dt.Name = pp.DataType) = 0 THEN (select oid from pg_catalog.pg_type WHERE typname = 'void') WHEN (SELECT count(dt.oid) FROM SYS." + type + "Params pp, pg_catalog.matpg_datatype dt WHERE pp." + type + "Name = t1.Name AND pp.SchemaName = t1.SchemaName AND pp." + type + "UID = t1.UID AND pp.Type = 'ResultSet' AND dt.Name = pp.DataType) > 0 THEN (select oid from pg_catalog.pg_type WHERE typname = 'record') " + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
        "ELSE (SELECT dt.oid FROM SYS." + type + "Params pp, pg_catalog.matpg_datatype dt WHERE pp." + type + "Name = t1.Name AND pp.SchemaName = t1.SchemaName AND pp." + type + "UID = t1.UID AND pp." + type + "UID = t1.UID AND pp.Type = 'ReturnValue' AND dt.Name = pp.DataType) END as prorettype,  " + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        "convert((SELECT count(*) FROM SYS." + type + "Params pp WHERE pp." + type + "Name = t1.Name AND pp.SchemaName = t1.SchemaName AND pp." + type + "UID = t1.UID AND pp.Type IN ('In', 'InOut')), short) as pronargs, " + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        "asPGVector((select "+arrayAgg("y.oid","y.type, y.position" )+" FROM ("+paramTable("'ResultSet','ReturnValue', 'Out'", type)+") as y)) as proargtypes, " +//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
        "(select "+arrayAgg("y.name", "y.type, y.position")+" FROM (SELECT pp.Name as name, pp.position as position, pp.Type as type FROM SYS." + type + "Params pp WHERE pp." + type + "Name = t1.Name AND pp.SchemaName = t1.SchemaName AND pp." + type + "UID = t1.UID AND pp.Type NOT IN ('ReturnValue' )) as y) as proargnames, " + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
        "(select case WHEN count(distinct(y.type)) = 1 THEN null ELSE "+arrayAgg("CASE WHEN (y.type ='In') THEN cast('i' as char) WHEN (y.type = 'Out') THEN cast('o' as char) WHEN (y.type = 'InOut') THEN cast('b' as char) WHEN (y.type = 'ResultSet') THEN cast('t' as char) END", "y.type,y.position")+" END FROM (SELECT pp.Type as type, pp.Position as position FROM SYS." + type + "Params pp WHERE pp." + type + "Name = t1.Name AND pp.SchemaName = t1.SchemaName AND pp." + type + "UID = t1.UID AND pp.Type NOT IN ('ReturnValue')) as y) as proargmodes, " + //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
        "(select case WHEN count(distinct(y.oid)) = 1 THEN null ELSE "+arrayAgg("y.oid", "y.type, y.position")+" END FROM ("+paramTable("'ReturnValue'", type)+") as y) as proallargtypes, " +  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
        "pg_catalog.getOid(t1.SchemaUID) as pronamespace " + //$NON-NLS-1$
        "FROM SYS." + type + "s as t1"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    private String paramTable(String notIn, String type) {
        return "SELECT case when pp.Type <> 'ResultSet' AND pp.DataType = 'object' then 2283 else dt.oid end as oid, pp.Position as position, pp.Type as type FROM SYS." + type + "Params pp LEFT JOIN pg_catalog.matpg_datatype dt ON pp.DataType=dt.Name " + //$NON-NLS-1$ //$NON-NLS-2$
                "WHERE pp." + type + "Name = t1.Name AND pp.SchemaName = t1.SchemaName AND pp." + type + "UID = t1.UID AND pp.Type NOT IN ("+notIn+")"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }


    private String arrayAgg(String select, String orderby) {
        return "array_agg("+select+" ORDER BY "+orderby+")";  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // triggers
    private Table add_pg_trigger()  {
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
    private Table add_pg_type() {
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
        //      typtype is b for a base type, c for a composite type (e.g., a table's row type), d for a domain,
        // e for an enum type, or p for a pseudo-type. See also typrelid and typbasetype
        addColumn("typtype", DataTypeManager.DefaultDataTypes.CHAR, t); //$NON-NLS-1$
        // typnotnull represents a not-null constraint on a type. Used for domains only.
        addColumn("typnotnull", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$
        // if this is a domain (see typtype), then typbasetype identifies the type that this one is based on.
        // Zero if this type is not a domain
        addColumn("typbasetype", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        // Domains use typtypmod to record the typmod to be applied to their base type
        // (-1 if base type does not use a typmod). -1 if this type is not a domain
        addColumn("typtypmod", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("typdelim", DataTypeManager.DefaultDataTypes.CHAR, t); //$NON-NLS-1$
        addColumn("typrelid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("typelem", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("typinput", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("typreceive", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("typdefault", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        //non-pg column to associate the teiid type name - this is expected to be unique.
        //aliases are handled by matpg_datatype
        addColumn("teiid_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$

        String transformation =
            "select oid, typname, (SELECT pg_catalog.getOid(uid) FROM SYS.Schemas where Name = 'SYS') as typnamespace, "  //$NON-NLS-1$ --we use SYS, but pg uses public
            + "typlen, typtype, false as typnotnull, typbasetype, typtypmod, cast(',' as char) as typdelim, typrelid, " //$NON-NLS-1$
            + "typelem, null as typinput, 2147483647 - cast(row_number() over (order by typname) as integer) as typreceive, null as typdfault, teiid_name from texttable('" + //$NON-NLS-1$
            "16,bool,1,b,0,-1,0,0,boolean\n" + //$NON-NLS-1$
            "17,bytea,-1,b,0,-1,0,0,blob\n" + //$NON-NLS-1$
            "18,char,1,b,0,-1,0,0,\n" + //$NON-NLS-1$
            "1043,varchar,-1,b,0,-1,0,0,string\n" + //$NON-NLS-1$
            "25,text,-1,b,0,-1,0,0,clob\n" + //$NON-NLS-1$
            "1042,bpchar,-1,b,0,-1,0,0,\n" + //$NON-NLS-1$
            "21,int2,2,b,0,-1,0,0,short\n" + //$NON-NLS-1$
            "20,int8,8,b,0,-1,0,0,long\n" + //$NON-NLS-1$
            "23,int4,4,b,0,-1,0,0,integer\n" + //$NON-NLS-1$
            "26,oid,4,b,0,-1,0,0,\n" + //$NON-NLS-1$
            "700,float4,4,b,0,-1,0,0,float\n" + //$NON-NLS-1$
            "701,float8,8,b,0,-1,0,0,double\n" + //$NON-NLS-1$
            "705,unknown,-2,b,0,-1,0,0,object\n" + //$NON-NLS-1$
            "1082,date,4,b,0,-1,0,0,date\n" + //$NON-NLS-1$
            "1083,time,8,b,0,-1,0,0,time\n" + //$NON-NLS-1$
            "1114,timestamp,8,b,0,-1,0,0,timestamp\n" + //$NON-NLS-1$
            "1700,numeric,-1,b,0,-1,0,0,bigdecimal\n" + //$NON-NLS-1$
            "142,xml,-1,b,0,-1,0,0,xml\n" + //$NON-NLS-1$
            "14939,lo,-1,b,0,-1,0,0,\n" + //$NON-NLS-1$
            "3803,json,-1,b,0,-1,0,0,json\n" + //$NON-NLS-1$
            "32816,geometry,-1,b,0,-1,0,0,geometry\n" + //$NON-NLS-1$
            "33454,geography,-1,b,0,-1,0,0,geography\n" + //$NON-NLS-1$
            "2278,void,4,p,0,-1,0,0,\n" + //$NON-NLS-1$
            "2249,record,-1,p,0,-1,0,0,\n" + //$NON-NLS-1$
            "30,oidvector,-1,b,0,-1,0,26,\n" + //$NON-NLS-1$
            "1000,_bool,-1,b,0,-1,0,16,boolean[]\n" + //$NON-NLS-1$
            "1001,_bytea,-1,b,0,-1,0,17,blob[]\n" + //$NON-NLS-1$
            "1002,_char,-1,b,0,-1,0,18,\n" + //$NON-NLS-1$
            "1005,_int2,-1,b,0,-1,0,21,short[]\n" + //$NON-NLS-1$
            "1007,_int4,-1,b,0,-1,0,23,integer[]\n" + //$NON-NLS-1$
            "1009,_text,-1,b,0,-1,0,25,clob[]\n" + //$NON-NLS-1$
            "1028,_oid,-1,b,0,-1,0,26,\n" + //$NON-NLS-1$
            "1014,_bpchar,-1,b,0,-1,0,1042,\n" + //$NON-NLS-1$
            "1015,_varchar,-1,b,0,-1,0,1043,string[]\n" + //$NON-NLS-1$
            "1016,_int8,-1,b,0,-1,0,20,long[]\n" + //$NON-NLS-1$
            "1021,_float4,-1,b,0,-1,0,700,float[]\n" + //$NON-NLS-1$
            "1022,_float8,-1,b,0,-1,0,701,double[]\n" + //$NON-NLS-1$
            "1031,_numeric,-1,b,0,-1,0,1700,bigdecimal[]\n" + //$NON-NLS-1$
            "1115,_timestamp,-1,b,0,-1,0,1114,timestamp[]\n" + //$NON-NLS-1$
            "1182,_date,-1,b,0,-1,0,1082,date[]\n" + //$NON-NLS-1$
            "1183,_time,-1,b,0,-1,0,1083,time[]\n" + //$NON-NLS-1$
            "3811,_json,-1,b,0,-1,0,3811,json[]\n" + //$NON-NLS-1$
            "32824,_geometry,-1,b,0,-1,0,32816,geometry[]\n" + //$NON-NLS-1$
            "33462,_geography,-1,b,0,-1,0,33454,geography[]\n" + //$NON-NLS-1$
            "143,_xml,-1,b,0,-1,0,142,xml[]\n" + //$NON-NLS-1$
            "2287,_record,-1,b,0,-1,0,2249,\n" + //$NON-NLS-1$
            "2283,anyelement,4,p,0,-1,0,0,\n" + //$NON-NLS-1$
            "22,int2vector,-1,b,0,-1,0,0," + //$NON-NLS-1$
            "2281,internal,8,p,0,-1,0,0," + //$NON-NLS-1$
            "' columns oid integer, typname string, typlen short, typtype char, typbasetype integer, typtypmod integer, typrelid integer, typelem integer, teiid_name string) AS t"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);
        t.setMaterialized(true);
        t.setProperty(MaterializationMetadataRepository.ALLOW_MATVIEW_MANAGEMENT, "true"); //$NON-NLS-1$
        return t;
    }

    private Table add_pg_database()  {
        Table t = createView("pg_database"); //$NON-NLS-1$
        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("datname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("encoding", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("datlastsysoid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        // this is is boolean type but the query coming in is in the form dataallowconn = 't'
        addColumn("datallowconn", DataTypeManager.DefaultDataTypes.CHAR, t); //$NON-NLS-1$
        addColumn("datconfig", DataTypeManager.DefaultDataTypes.OBJECT, t); //$NON-NLS-1$
        addColumn("datacl", "object[]", t); //$NON-NLS-1$ //$NON-NLS-2$
        addColumn("datdba", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("dattablespace", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        String transformation = "SELECT 0 as oid, " + //$NON-NLS-1$
                "current_database() as datname, " + //$NON-NLS-1$
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

    private Table add_pg_user()  {
        Table t = createView("pg_user"); //$NON-NLS-1$
        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("usename", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("usesysid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("usecreatedb", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$
        addColumn("usesuper", DataTypeManager.DefaultDataTypes.BOOLEAN, t); //$NON-NLS-1$

        String transformation = "SELECT 0 as oid, " + //$NON-NLS-1$
                "user(false) as usename, 0 as usesysid," + //$NON-NLS-1$
                "false as usecreatedb, " + //$NON-NLS-1$
                "false as usesuper "; //$NON-NLS-1$
        t.setSelectTransformation(transformation);
        return t;
    }

    private Table add_matpg_datatype()  {
        Table t = createView("matpg_datatype"); //$NON-NLS-1$
        addColumn("oid", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("typname", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("typlen", DataTypeManager.DefaultDataTypes.SHORT, t); //$NON-NLS-1$
        addColumn("typtype", DataTypeManager.DefaultDataTypes.CHAR, t); //$NON-NLS-1$
        addColumn("typbasetype", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("typtypmod", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$

        addPrimaryKey("matpg_datatype_names", Arrays.asList("oid", "name"), t); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        addIndex("matpg_datatype_ids", true, Arrays.asList("typname", "oid"), t); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        String transformation = "select pt.oid as oid, pt.typname as typname, pt.teiid_name as name, pt.typlen, pt.typtype, pt.typbasetype, pt.typtypmod from pg_catalog.pg_type pt" //$NON-NLS-1$
                + " UNION ALL select pt.oid as oid, pt.typname as typname, 'char' as name, pt.typlen, pt.typtype, pt.typbasetype, pt.typtypmod from pg_catalog.pg_type pt where typname='bpchar'" //$NON-NLS-1$
                + " UNION ALL select pt.oid as oid, pt.typname as typname, 'byte' as name, pt.typlen, pt.typtype, pt.typbasetype, pt.typtypmod from pg_catalog.pg_type pt where typname='int2'" //$NON-NLS-1$
                + " UNION ALL select pt.oid as oid, pt.typname as typname, 'biginteger' as name, pt.typlen, pt.typtype, pt.typbasetype, pt.typtypmod from pg_catalog.pg_type pt where typname='numeric'"  //$NON-NLS-1$
                + " UNION ALL select pt.oid as oid, pt.typname as typname, 'varbinary' as name, pt.typlen, pt.typtype, pt.typbasetype, pt.typtypmod from pg_catalog.pg_type pt where typname='bytea'"  //$NON-NLS-1$
                + " UNION ALL select pt.oid as oid, pt.typname as typname, 'byte[]' as name, pt.typlen, pt.typtype, pt.typbasetype, pt.typtypmod from pg_catalog.pg_type pt where typname='_int2'" //$NON-NLS-1$
                + " UNION ALL select pt.oid as oid, pt.typname as typname, 'biginteger[]' as name, pt.typlen, pt.typtype, pt.typbasetype, pt.typtypmod from pg_catalog.pg_type pt where typname='_numeric'"  //$NON-NLS-1$
                + " UNION ALL select pt.oid as oid, pt.typname as typname, 'varbinary[]' as name, pt.typlen, pt.typtype, pt.typbasetype, pt.typtypmod from pg_catalog.pg_type pt where typname='_bytea'"  //$NON-NLS-1$
                + " UNION ALL select pt.oid as oid, pt.typname as typname, 'char[]' as name, pt.typlen, pt.typtype, pt.typbasetype, pt.typtypmod from pg_catalog.pg_type pt where typname='_bpchar'"; //$NON-NLS-1$
        t.setSelectTransformation(transformation);
        t.setMaterialized(true);
        t.setProperty(MaterializationMetadataRepository.ALLOW_MATVIEW_MANAGEMENT, "true"); //$NON-NLS-1$
        return t;
    }

    private Table add_infoSchemaViews() {
        Table t = createView("information_schema.views"); //$NON-NLS-1$
        addColumn("table_catalog", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_schema", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("check_option", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("is_updatable", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        t.setSelectTransformation("select vdbname, schemaName, name, 'NONE', case when SupportsUpdates then 'YES' else 'NO' end from sys.tables where NOT IsPhysical"); //$NON-NLS-1$
        return t;
    }

    private Table add_infoSchemaTables() {
        Table t = createView("information_schema.tables"); //$NON-NLS-1$
        addColumn("table_catalog", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_schema", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_type", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        t.setSelectTransformation("select vdbname, schemaName, name," + //$NON-NLS-1$
                " case when IsSystem then 'SYSTEM ' else '' end || case when IsPhysical then 'BASE TABLE' else 'VIEW' end from sys.tables"); //$NON-NLS-1$
        return t;
    }

    private Table add_infoSchemaColumns() {
        Table t = createView("information_schema.columns"); //$NON-NLS-1$
        addColumn("table_catalog", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_schema", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("column_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("ordinal_position", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("column_default", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("is_nullable", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("udt_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("data_type", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("character_maximum_length", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("character_octet_length", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("numeric_precision", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("numeric_precision_radix", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("numeric_scale", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("datetime_precision", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("character_set_catalog", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("character_set_schema", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("character_set_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("collation_catalog", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("is_updatable", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        t.setSelectTransformation("select vdbname, schemaName, tableName, columns.name, position, defaultValue," + //$NON-NLS-1$
                " case when NullType = 'No Nulls' then 'NO' else 'YES' end," + //$NON-NLS-1$
                " typname, case WHEN endswith(']', columns.DataType) then 'ARRAY' else pg_catalog.format_type( pt.oid, 0) end, case WHEN columns.DataType in ('string', 'char') THEN columns.length end, null," + //$NON-NLS-1$
                " CASE WHEN columns.DataType in ('bigdecimal', 'biginteger') then columns.precision WHEN columns.DataType = 'float' then 24 WHEN columns.DataType = 'double' then 53 WHEN columns.DataType in ('byte', 'short', 'integer', 'long') then pt.typlen * 8 end," + //$NON-NLS-1$
                " CASE WHEN columns.DataType in ('bigdecimal', 'biginteger', 'float', 'double', 'byte', 'short', 'integer', 'long') then 2 end, " + //$NON-NLS-1$
                " CASE WHEN columns.DataType in ('bigdecimal', 'biginteger') then columns.scale WHEN columns.DataType in ('byte', 'short', 'integer', 'long') then 0 end," + //$NON-NLS-1$
                " CASE WHEN columns.DataType = 'date' then 0 WHEN columns.DataType = 'time' then 3 WHEN columns.DataType = 'timestamp' then 9 end," + //$NON-NLS-1$
                " null, null, null, null," + //$NON-NLS-1$
                " case when SupportsUpdates then 'YES' else 'NO' end " + //$NON-NLS-1$
                " from sys.columns left outer join pg_catalog.matpg_datatype pt ON columns.DataType = pt.Name"); //$NON-NLS-1$
        return t;
    }

    private Table add_infoSchemaReferentialConstraints() {
        Table t = createView("information_schema.referential_constraints"); //$NON-NLS-1$
        addColumn("constraint_catalog", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("constraint_schema", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("constraint_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("unique_constraint_catalog", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("unique_constraint_schema", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("unique_constraint_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("match_option", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("update_rule", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("delete_rule", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        t.setSelectTransformation("select PKTABLE_CAT, PKTABLE_SCHEM, PK_NAME, FKTABLE_CAT, FKTABLE_SCHEM, FK_NAME, 'NONE', 'NO ACTION', 'NO ACTION' FROM SYS.ReferenceKeyColumns " //$NON-NLS-1$
                + "group by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FK_NAME, PK_NAME"); //$NON-NLS-1$
        return t;
    }

    private Table add_infoSchemaKeyColumnUsage() {
        Table t = createView("information_schema.key_column_usage"); //$NON-NLS-1$
        addColumn("constraint_catalog", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("constraint_schema", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("constraint_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_catalog", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_schema", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("column_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("ordinal_position", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        addColumn("position_in_unique_constraint", DataTypeManager.DefaultDataTypes.INTEGER, t); //$NON-NLS-1$
        t.setSelectTransformation("select VDBName, SchemaName, KeyName, VDBName, SchemaName, TableName, Name, KeyColumns.Position, case when RefKeyUID is not null then KeyColumns.Position end " //$NON-NLS-1$
                + "FROM SYS.KeyColumns"); //$NON-NLS-1$
        return t;
    }

    private Table add_infoSchemaTableConstraints() {
        Table t = createView("information_schema.table_constraints"); //$NON-NLS-1$
        addColumn("constraint_catalog", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("constraint_schema", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("constraint_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_catalog", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_schema", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("table_name", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("constraint_type", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("is_deferrable", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        addColumn("initially_deferred", DataTypeManager.DefaultDataTypes.STRING, t); //$NON-NLS-1$
        t.setSelectTransformation("select VDBName, SchemaName, Name, VDBName, SchemaName, TableName, upper(Keys.Type || case when Keys.Type in ('Primary', 'Foreign') then ' Key' else '' end), 'NO', 'NO' " //$NON-NLS-1$
                + "FROM SYS.Keys where Keys.Type in ('Primary', 'Foreign', 'Unique')"); //$NON-NLS-1$
        return t;
    }

    private FunctionMethod addFunction(String javaFunction, String name) {
        Method[] methods = FunctionMethods.class.getMethods();
        for (Method method : methods) {
            if (!method.getName().equals(javaFunction)) {
                continue;
            }
            FunctionMethod func = addFunction(name, method);
            func.setCategory("pg"); //$NON-NLS-1$
            func.setDescription(name);
            return func;
        }
        throw new AssertionError("Could not find function"); //$NON-NLS-1$
    }

    //TODO use the TeiidFunction annotation instead
    public static class FunctionMethods {
        public static ClobType encode(BlobType value, String encoding) throws SQLException, IOException {
            return org.teiid.query.function.FunctionMethods.toChars(value, encoding);
        }

        public static String postgisLibVersion() {
            return POSTGIS_LIB_VERSION;
        }

        public static String postgisVersion() {
            return "2.0.0"; //$NON-NLS-1$
        }

        public static String postgisGeosVersion() {
            return null;
        }

        public static String postgisProjVersion() {
            return "Rel. 4.8.0"; //$NON-NLS-1$
        }

        public static Boolean hasPerm(@SuppressWarnings("unused") Integer oid,
                @SuppressWarnings("unused") String permission) {
            return true;
        }

        public static String getExpr2(String text,
                @SuppressWarnings("unused") Integer oid) {
            return text;
        }

        public static String getExpr3(String text,
                @SuppressWarnings("unused") Integer oid,
                @SuppressWarnings("unused") Boolean prettyPrint) {
            return text;
        }

        public static Object asPGVector(Object obj) {
            if (obj instanceof ArrayImpl) {
                ((ArrayImpl)obj).setZeroBased(true);
            }
            return obj;
        }

        public static Integer getOid(org.teiid.CommandContext cc, String uid) {
            VDBMetaData metadata = (VDBMetaData) cc.getVdb();
            TransformationMetadata tm = metadata.getAttachment(TransformationMetadata.class);
            return tm.getMetadataStore().getOid(uid);
        }

        public static String pg_client_encoding(org.teiid.CommandContext cc) {
            SessionMetadata session = (SessionMetadata)cc.getSession();
            ODBCServerRemoteImpl server = session.getAttachment(ODBCServerRemoteImpl.class);
            String encoding = null;
            if (server != null) {
                encoding = server.getEncoding();
            }
            if (encoding == null) {
                return PgBackendProtocol.DEFAULT_ENCODING;
            }
            return encoding;
        }

        public static Integer regClass(org.teiid.CommandContext cc, String name) throws TeiidComponentException, QueryResolverException, org.teiid.query.parser.ParseException {
            VDBMetaData metadata = (VDBMetaData) cc.getVdb();
            TransformationMetadata tm = metadata.getAttachment(TransformationMetadata.class);
            GroupSymbol symbol = new GroupSymbol(SQLParserUtil.normalizeId(name));
            ResolverUtil.resolveGroup(symbol, tm);
            return tm.getMetadataStore().getOid(((Table)symbol.getMetadataID()).getUUID());
        }

        public static String objDescription(org.teiid.CommandContext cc, int oid) {
            //TODO need a reverse lookup by oid at least for schema or add the annotation to pg_namespace
            return null;
        }

        public static String objDescription2(org.teiid.CommandContext cc, int oid, String type) {
            //TODO need a reverse lookup by oid at least for schema or add the annotation to pg_namespace
            return null;
        }

        public static String getUserById(int user) {
            return "pgadmin"; //$NON-NLS-1$
        }

        public static boolean hasSchemaPrivilege(org.teiid.CommandContext cc, String name, String privilege) {
            //TODO: could check if the schema exists
            return "usage".equalsIgnoreCase(privilege); //$NON-NLS-1$
        }

        public static boolean hasTablePrivilege(org.teiid.CommandContext cc, String name, String privilege) {
            //TODO: check against authorizationvalidator
            return true;
        }

        public static String currentSchema(org.teiid.CommandContext cc) {
            return "SYS"; //$NON-NLS-1$
        }

        public static String formatType(org.teiid.CommandContext cc, Integer oid, Integer typmod) throws SQLException {
            if (oid == null) {
                return null;
            }
            Connection c = cc.getConnection();
            try {
                PreparedStatement ps = c.prepareStatement("select typname from pg_catalog.pg_type where oid = ?"); //$NON-NLS-1$
                ps.setInt(1, oid);
                ps.execute();
                ResultSet rs = ps.getResultSet();
                if (rs.next()) {
                    String name = rs.getString(1);
                    boolean isArray = name.startsWith("_"); //$NON-NLS-1$
                    if (isArray) {
                        name = name.substring(1);
                    }
                    switch (name) {
                    case "bool": //$NON-NLS-1$
                        name = "boolean"; //$NON-NLS-1$
                        break;
                    case "varchar": //$NON-NLS-1$
                        name = "character varying"; //$NON-NLS-1$
                        break;
                    case "int2": //$NON-NLS-1$
                        name = "smallint"; //$NON-NLS-1$
                        break;
                    case "int4": //$NON-NLS-1$
                        name = "integer"; //$NON-NLS-1$
                        break;
                    case "int8": //$NON-NLS-1$
                        name = "bigint"; //$NON-NLS-1$
                        break;
                    case "float4": //$NON-NLS-1$
                        name = "real"; //$NON-NLS-1$
                        break;
                    case "float8": //$NON-NLS-1$
                        name = "double precision"; //$NON-NLS-1$
                        break;
                    case "bpchar": //$NON-NLS-1$
                        name = "character"; //$NON-NLS-1$
                        break;
                    }
                    if (typmod != null && typmod > 4) {
                        if (name.equals("numeric")) {  //$NON-NLS-1$
                            name += "("+((typmod-4)>>16)+","+((typmod-4)&0xffff)+")";  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        } else if (name.equals("character") || name.equals("varchar")) { //$NON-NLS-1$ //$NON-NLS-2$
                            name += "("+(typmod-4)+")";  //$NON-NLS-1$ //$NON-NLS-2$
                        }
                    }
                    if (isArray) {
                        name += "[]"; //$NON-NLS-1$
                    }
                    return name;
                }
                return "???"; //$NON-NLS-1$
            } finally {
                if (c != null) {
                    c.close();
                }
            }
        }

        public static String colDescription(org.teiid.CommandContext cc, int oid, int column_number) {
            //TODO need a reverse lookup by oid or add the annotation to pg_attribute
            return null;
        }

        public static boolean pgHasRole(org.teiid.CommandContext cc, int userOid, String privilege) {
            return true;
        }

        public static BlobType asBinary2(GeometryType geom, String encoding) throws FunctionExecutionException {
            return GeometryFunctionMethods.asBlob(geom, encoding);
        }

        public static BlobType asBinary3(GeographyType geog, String encoding) throws FunctionExecutionException {
            return GeometryFunctionMethods.asBlob(geog, encoding);
        }

        public static boolean pg_table_is_visible(int oid) throws FunctionExecutionException {
            return true;
        }

        public static String pg_get_constraintdef(org.teiid.CommandContext cc, int oid, boolean pretty) throws SQLException {
            //return a simple constraint def
            try (Connection c = cc.getConnection(); PreparedStatement ps = c.prepareStatement("select pkcolumn_name, pktable_schem, pktable_name, fkcolumn_name from REFERENCEKEYCOLUMNS where getoid(fk_uid) = ? order by KEY_SEQ")) { //$NON-NLS-1$
                ps.setInt(1, oid);
                ps.execute();
                ResultSet rs = ps.getResultSet();
                String refTable = null;
                List<String> columnNames = new ArrayList<String>();
                List<String> refColumnNames = new ArrayList<String>();
                while (rs.next()) {
                    if (refTable == null) {
                        refTable = SQLStringVisitor.escapeSinglePart(rs.getString(2)) + SQLConstants.Tokens.DOT + SQLStringVisitor.escapeSinglePart(rs.getString(3));
                    }
                    columnNames.add(SQLStringVisitor.escapeSinglePart(rs.getString(4)));
                    refColumnNames.add(SQLStringVisitor.escapeSinglePart(rs.getString(1)));
                }
                if (refTable == null) {
                    return null;
                }
                return "FOREIGN KEY (" + StringUtil.join(columnNames, ",")+ ") REFERENCES " + refTable + "("+ StringUtil.join(refColumnNames, ",") + ")"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$  //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
            }
        }

        public static boolean pg_type_is_visible(int oid) throws FunctionExecutionException {
            return true;
        }

        public static String version() {
            return POSTGRESQL_VERSION;
        }

        public static String pg_encoding_to_char(int code) {
            if (code == 6) {
                return "UTF8"; //$NON-NLS-1$
            }
            throw new AssertionError("Unknown encoding"); //$NON-NLS-1$
        }

        public static String[] current_schemas(CommandContext cc, boolean include_implicit) {
            //TODO: when schemas are hidden we should use the system metadata or other logic to hide them here as well
            if (include_implicit) {
                VDBMetaData metadata = (VDBMetaData) cc.getVdb();
                TransformationMetadata tm = metadata.getAttachment(TransformationMetadata.class);
                List<Schema> schemas = tm.getMetadataStore().getSchemaList();
                String[] results = new String[schemas.size()];
                for (int i = 0; i < results.length; i++) {
                    results[i] = schemas.get(i).getName();
                }
                return results;
            }
            List<Model> models = cc.getVdb().getModels();
            String[] results = new String[models.size()];
            for (int i = 0; i < results.length; i++) {
                results[i] = models.get(i).getName();
            }
            return results;
        }

        public static String pg_get_indexdef(CommandContext cc, int index_oid, short column_no, boolean pretty_bool) throws TeiidSQLException, SQLException {
            //TODO: account for function based index
            try (Connection c = cc.getConnection(); PreparedStatement ps = c.prepareStatement("select indkey_names[?] from pg_index where indexrelid = ?")) { //$NON-NLS-1$
                ps.setShort(1, column_no);
                ps.setInt(2, index_oid);
                ps.execute();
                ResultSet rs = ps.getResultSet();
                String result = null;
                if (rs.next()) {
                    result = rs.getString(1);
                }
                return result;
            }
        }

        public static String array_to_string(CommandContext cc, Object[] array, String delim) {
            return StringUtil.join(Arrays.asList(array).stream()
                    .map(o -> {
                        try {
                            return (String)DataTypeManager.transformValue(o, DataTypeManager.DefaultDataClasses.STRING);
                        } catch (TransformationException e) {
                            throw new TeiidRuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList()), delim);
        }

        public static Long pg_total_relation_size(Integer reloid) {
            return null;
        }

        public static Long pg_relation_size(Integer reloid) {
            return null;
        }

        public static Long pg_stat_get_numscans(Integer reloid) {
            return 0L;
        }

        public static String pg_get_ruledef(Integer ruleoid) {
            return null;
        }

        //public static int lo_open(int lobjId, int mode) {
        //
        //}

    }
}
