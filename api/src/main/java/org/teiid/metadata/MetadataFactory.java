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

package org.teiid.metadata;

import java.io.Reader;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.teiid.CommandContext;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.connector.DataPlugin;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.StringUtil;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.TypeFacility;


/**
 * Allows connectors to build metadata for use by the engine.
 * 
 * TODO: add support for datatype import
 * TODO: add support for unique constraints
 */
public class MetadataFactory implements Serializable {
	private static final String TEIID_RESERVED = "teiid_"; //$NON-NLS-1$
	private static final String TEIID_SF = "teiid_sf"; //$NON-NLS-1$
	private static final String TEIID_RELATIONAL = "teiid_rel"; //$NON-NLS-1$
	private static final String TEIID_WS = "teiid_ws"; //$NON-NLS-1$

	private static final long serialVersionUID = 8590341087771685630L;
	
	private String vdbName;
	private int vdbVersion;
	private Map<String, Datatype> dataTypes;
	private Map<String, Datatype> builtinDataTypes;
	private boolean autoCorrectColumnNames = true;
	private Map<String, String> namespaces;
	private String rawMetadata;
	private Properties modelProperties;
	private Schema schema = new Schema();
	private String idPrefix; 
	protected int count;
	private transient Parser parser;
	private transient ModelMetaData model;
	
	public static final String SF_URI = "{http://www.teiid.org/translator/salesforce/2012}"; //$NON-NLS-1$
	public static final String WS_URI = "{http://www.teiid.org/translator/ws/2012}"; //$NON-NLS-1$
	
	public static final Map<String, String> BUILTIN_NAMESPACES;
	static {
		Map<String, String> map = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
		map.put(TEIID_RELATIONAL, AbstractMetadataRecord.RELATIONAL_URI.substring(1, AbstractMetadataRecord.RELATIONAL_URI.length()-1));
		map.put(TEIID_SF, SF_URI.substring(1, SF_URI.length()-1));
		map.put(TEIID_WS, WS_URI.substring(1, WS_URI.length()-1));
		BUILTIN_NAMESPACES = Collections.unmodifiableMap(map);
	}
	
	public MetadataFactory(String vdbName, int vdbVersion, Map<String, Datatype> runtimeTypes, ModelMetaData model) {
		this(vdbName, vdbVersion, model.getName(), runtimeTypes, model.getProperties(), model.getSchemaText());
		this.model = model;
	}
	
	public MetadataFactory(String vdbName, int vdbVersion, String schemaName, Map<String, Datatype> runtimeTypes, Properties modelProperties, String rawMetadata) {
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
		this.dataTypes = runtimeTypes;
		this.builtinDataTypes = runtimeTypes;
		schema.setName(schemaName);
		long msb = longHash(vdbName, 0);
		msb = 31*msb + vdbVersion;
		msb = longHash(schemaName, msb);
		idPrefix = "tid:" + hex(msb, 12); //$NON-NLS-1$
		setUUID(schema);	
		this.modelProperties = modelProperties;
		this.rawMetadata = rawMetadata;
	}
	
	private long longHash(String s, long h) {
		if (s == null) {
			return h;
		}
		for (int i = 0; i < s.length(); i++) {
            h = 31*h + s.charAt(i);
        }
		return h;
	}
	
	private static String hex(long val, int hexLength) {
		long hi = 1L << (hexLength * 4);
		return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }

	/**
	 * @deprecated
	 * @return
	 * @see #getModelProperties()
	 */
	public Properties getImportProperties() {
		return modelProperties;
	}
	
	public Properties getModelProperties() {
		return modelProperties;
	}
	
	public String getRawMetadata() {
		return this.rawMetadata;
	}
	
	public Model getModel() {
		return model;
	}
	
	protected void setUUID(AbstractMetadataRecord record) {
		int lsb = 0;
		if (record.getParent() != null) {
			lsb  = record.getParent().getUUID().hashCode();
		}
		lsb = 31*lsb + record.getName().hashCode();
		String uuid = idPrefix+"-"+hex(lsb, 8) + "-" + hex(count++, 8); //$NON-NLS-1$ //$NON-NLS-2$
        record.setUUID(uuid); 
	}
	
	public String getName() {
		return this.schema.getName();
	}
	
	public Schema getSchema() {
		return this.schema;
	}

	/**
	 * Add a table with the given name to the model.  
	 * @param name
	 * @return
	 * @throws MetadataException 
	 */
	public Table addTable(String name) {
		Table table = new Table();
		table.setTableType(Table.Type.Table);
		table.setName(name);
		setUUID(table);
		schema.addTable(table);
		return table;
	}
	
	/**
	 * Adds a column to the table with the given name and type.
	 * @param name
	 * @param type should be one of {@link TypeFacility.RUNTIME_NAMES}
	 * @param table
	 * @return
	 * @throws MetadataException
	 */
	public Column addColumn(String name, String type, ColumnSet<?> table) {
		if (autoCorrectColumnNames) {
			name.replace(AbstractMetadataRecord.NAME_DELIM_CHAR, '_');
		} else if (name.indexOf(AbstractMetadataRecord.NAME_DELIM_CHAR) != -1) {
			throw new MetadataException(DataPlugin.Event.TEIID60008, DataPlugin.Util.gs(DataPlugin.Event.TEIID60008, name));
		}
		if (table.getColumnByName(name) != null) {
			throw new DuplicateRecordException(DataPlugin.Event.TEIID60016, DataPlugin.Util.gs(DataPlugin.Event.TEIID60016, table.getFullName() + AbstractMetadataRecord.NAME_DELIM_CHAR + name));
		}
		Column column = new Column();
		column.setName(name);
		table.addColumn(column);
		column.setParent(table);
		column.setPosition(table.getColumns().size()); //1 based indexing
		setColumnType(type, column);
		setUUID(column);
		return column;
	}

	private Datatype setColumnType(String type,
			BaseColumn column) {
		Datatype datatype = dataTypes.get(type);
		if (datatype == null) {
			 throw new MetadataException(DataPlugin.Event.TEIID60009, DataPlugin.Util.gs(DataPlugin.Event.TEIID60009, type));
		}
		column.setDatatype(datatype, true);
		return datatype;
	}
	
	/**
	 * Adds a primary key to the given table.  The column names should be in key order.
	 * @param name
	 * @param columnNames
	 * @param table
	 * @return
	 * @throws MetadataException
	 */
	public KeyRecord addPrimaryKey(String name, List<String> columnNames, Table table) {
		KeyRecord primaryKey = new KeyRecord(KeyRecord.Type.Primary);
		primaryKey.setParent(table);
		primaryKey.setColumns(new ArrayList<Column>(columnNames.size()));
		primaryKey.setName(name);
		setUUID(primaryKey);
		assignColumns(columnNames, table, primaryKey);
		table.setPrimaryKey(primaryKey);
		return primaryKey;
	}
	
	/**
	 * Adds an access pattern to the given table.
	 * @param name
	 * @param columnNames
	 * @param table
	 * @return
	 * @throws MetadataException
	 */
	public KeyRecord addAccessPattern(String name, List<String> columnNames, Table table) {
		KeyRecord ap = new KeyRecord(KeyRecord.Type.AccessPattern);
		ap.setParent(table);
		ap.setColumns(new ArrayList<Column>(columnNames.size()));
		ap.setName(name);
		setUUID(ap);
		assignColumns(columnNames, table, ap);
		table.getAccessPatterns().add(ap);
		return ap;
	}	
	
	/**
	 * Adds an index to the given table.
	 * @param name
	 * @param nonUnique true indicates that an index is being added.
	 * @param columnNames
	 * @param table
	 * @return
	 * @throws MetadataException
	 */
	public KeyRecord addIndex(String name, boolean nonUnique, List<String> columnNames, Table table) {
		KeyRecord index = new KeyRecord(nonUnique?KeyRecord.Type.Index:KeyRecord.Type.Unique);
		index.setParent(table);
		index.setColumns(new ArrayList<Column>(columnNames.size()));
		index.setName(name);
		assignColumns(columnNames, table, index);
		setUUID(index);
		if (nonUnique) {
			table.getIndexes().add(index);
		}
		else {
			table.getUniqueKeys().add(index);
		}
		return index;
	}
	
	/**
	 * Adds a function based index on the given expressions.
	 * @param name
	 * @param expressions
	 * @param nonColumnExpressions a Boolean list indicating when expressions are non-column expressions
	 * @param table
	 * @return
	 * @throws MetadataException
	 */
	public KeyRecord addFunctionBasedIndex(String name, List<String> expressions, List<Boolean> nonColumnExpressions, Table table) {
		KeyRecord index = new KeyRecord(KeyRecord.Type.Index);
		index.setParent(table);
		index.setColumns(new ArrayList<Column>(expressions.size()));
		index.setName(name);
		setUUID(index);
		boolean functionBased = false;
		for (int i = 0; i < expressions.size(); i++) {
			String expr = expressions.get(i);
			if (nonColumnExpressions.get(i)) {
				Column c = new Column();
				//TODO: we could choose a derived name at this point, but we delay that to get a single unique name across all index expressions
				c.setName(expr);
				c.setNameInSource(expr);
				setUUID(c);
				c.setParent(index);
				c.setPosition(i + 1); //position is temporarily relative to the index, but the validator changes this
				index.getColumns().add(c);
				functionBased = true;
			} else {
				assignColumn(table, index, expr);
			}
		}
		if (!functionBased) {
			table.getIndexes().add(index);
		} else {
			table.getFunctionBasedIndexes().add(index);
		}
		return index;
	}

	private void assignColumn(Table table, ColumnSet<?> columns, String columnName) {
		Column column = table.getColumnByName(columnName);
		if (column == null) {
			throw new MetadataException(DataPlugin.Event.TEIID60011, DataPlugin.Util.gs(DataPlugin.Event.TEIID60011, columnName));				
		}
		columns.getColumns().add(column);
	}
	
	/**
	 * Adds a foreign key to the given table.  The referenced primary key must already exist.  The column names should be in key order.
	 * @param name
	 * @param columnNames
	 * @param referenceTable - schema qualified reference table name
	 * @param table
	 * @return
	 * @throws MetadataException
	 */
	public ForeignKey addForiegnKey(String name, List<String> columnNames, String referenceTable, Table table) {
		return addForiegnKey(name, columnNames, null, referenceTable, table);
	}

	/**
	 * Adds a foreign key to the given table.  The referenced key may be automatically created if addUniqueConstraint is true. The column names should be in key order.
	 * if reference table is is another schema, they will be resolved during validation.
	 * @param name
	 * @param columnNames
	 * @param referencedColumnNames, may be null to indicate that the primary key should be used.
	 * @param referenceTable - schema qualified reference table name, can be from another schema
	 * @param table
	 * @param addUniqueConstraint - if true, if the referenced table columns do not match with either PK, or FK then a UNIQUE index on reference table is created.
	 * @return
	 * @throws MetadataException
	 */	
	public ForeignKey addForiegnKey(String name, List<String> columnNames, List<String> referencedColumnNames, String referenceTable, Table table) {
		ForeignKey foreignKey = new ForeignKey();
		foreignKey.setParent(table);
		foreignKey.setColumns(new ArrayList<Column>(columnNames.size()));
		assignColumns(columnNames, table, foreignKey);
		foreignKey.setReferenceTableName(referenceTable);
		foreignKey.setReferenceColumns(referencedColumnNames);
		foreignKey.setName(name);
		setUUID(foreignKey);
		table.getForeignKeys().add(foreignKey);
		return foreignKey;
	}	
	
	/**
	 * Add a procedure with the given name to the model.  
	 * @param name
	 * @return
	 * @throws MetadataException 
	 */
	public Procedure addProcedure(String name) {
		Procedure procedure = new Procedure();
		procedure.setName(name);
		setUUID(procedure);
		procedure.setParameters(new LinkedList<ProcedureParameter>());
		schema.addProcedure(procedure);
		return procedure;
	}
	
	/**
	 * Add a procedure parameter.
	 * @param name
	 * @param type should be one of {@link TypeFacility.RUNTIME_NAMES}
	 * @param parameterType should be one of {@link ProcedureParameter.Type}
	 * @param procedure
	 * @return
	 * @throws MetadataException 
	 */
	public ProcedureParameter addProcedureParameter(String name, String type, ProcedureParameter.Type parameterType, Procedure procedure) {
		ProcedureParameter param = new ProcedureParameter();
		param.setName(name);
		setUUID(param);
		param.setType(parameterType);
		param.setProcedure(procedure);
		setColumnType(type, param);
		if (parameterType == Type.ReturnValue) {
			procedure.getParameters().add(0, param);
			for (int i = 0; i < procedure.getParameters().size(); i++) {
				procedure.getParameters().get(i).setPosition(i+1); //1 based indexing
			}
		} else {
			procedure.getParameters().add(param);
			param.setPosition(procedure.getParameters().size()); //1 based indexing
		}
		return param;
	}
	
	/**
	 * Add a procedure resultset column to the given procedure.
	 * @param name
	 * @param type should be one of {@link TypeFacility.RUNTIME_NAMES}
	 * @param procedure
	 * @return
	 * @throws MetadataException 
	 */
	public Column addProcedureResultSetColumn(String name, String type, Procedure procedure) {
		if (procedure.getResultSet() == null) {
			ColumnSet<Procedure> resultSet = new ColumnSet<Procedure>();
			resultSet.setParent(procedure);
			resultSet.setName("RSParam"); //$NON-NLS-1$
			setUUID(resultSet);
			procedure.setResultSet(resultSet);
		}
		return addColumn(name, type, procedure.getResultSet());
	}

	private void assignColumns(List<String> columnNames, Table table,
			ColumnSet<?> columns) {
		for (String columnName : columnNames) {
			assignColumn(table, columns, columnName);
		}
	}
	
	/**
	 * Add a function with the given name to the model.  
	 * @param name
	 * @return
	 * @throws MetadataException 
	 */
	public FunctionMethod addFunction(String name) {
		FunctionMethod function = new FunctionMethod();
		function.setName(name);
		setUUID(function);
		schema.addFunction(function);
		return function;
	}
	
	/**
	 * Adds a non-pushdown function based upon the given {@link Method}.
	 * @param name
	 * @param method
	 * @return
	 * @throws MetadataException
	 */
	public FunctionMethod addFunction(String name, Method method) {
		FunctionMethod func = createFunctionFromMethod(name, method);
		setUUID(func);
		getSchema().addFunction(func);
		return func;
	}

	public static FunctionMethod createFunctionFromMethod(String name, Method method) {
		String returnType = DataTypeManager.getDataTypeName(method.getReturnType());
		Class<?>[] params = method.getParameterTypes();
		String[] paramTypes = new String[params.length];
		boolean nullOnNull = false;
		for (int i = 0; i < params.length; i++) {
			Class<?> clazz = params[i];
			if (clazz.isPrimitive()) {
				nullOnNull = true;
				clazz = TypeFacility.convertPrimitiveToObject(clazz);
			}
			if (method.isVarArgs() && i == params.length -1) {
				paramTypes[i] = DataTypeManager.getDataTypeName(clazz.getComponentType());
			} else {
				paramTypes[i] = DataTypeManager.getDataTypeName(clazz);
			}
		}
		if (params.length > 0 && CommandContext.class.isAssignableFrom(params[0])) {
			paramTypes = Arrays.copyOfRange(paramTypes, 1, paramTypes.length);
		}
		FunctionMethod func = FunctionMethod.createFunctionMethod(name, null, null, returnType, paramTypes);
		func.setInvocationMethod(method.getName());
		func.setPushdown(PushDown.CANNOT_PUSHDOWN);
		func.setMethod(method);
		func.setInvocationClass(method.getDeclaringClass().getName());
		func.setNullOnNull(nullOnNull);
		if (method.isVarArgs()) {
			func.setVarArgs(method.isVarArgs());
		}
		return func;
	}

	/**
	 * Set to false to disable correcting column and other names to be valid Teiid names.
	 * @param autoCorrectColumnNames
	 */
	public void setAutoCorrectColumnNames(boolean autoCorrectColumnNames) {
		this.autoCorrectColumnNames = autoCorrectColumnNames;
	}
	
	public void addNamespace(String prefix, String uri) {
		if (StringUtil.startsWithIgnoreCase(prefix, TEIID_RESERVED)) { 
			throw new MetadataException(DataPlugin.Event.TEIID60017, DataPlugin.Util.gs(DataPlugin.Event.TEIID60017, prefix));
		}
		if (uri == null || uri.indexOf('}') != -1) {
			throw new MetadataException(DataPlugin.Event.TEIID60018, DataPlugin.Util.gs(DataPlugin.Event.TEIID60018, uri));
		}
		if (this.namespaces == null) {
			 this.namespaces = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
		}
		this.namespaces.put(prefix, uri);
	}
	
	public void mergeInto (MetadataStore store) {
		store.addSchema(this.schema);
		store.addDataTypes(this.builtinDataTypes.values());
	}
	
	public MetadataStore asMetadataStore() {
		MetadataStore store = new MetadataStore();
		mergeInto(store);
		return store;
	}
	
	/**
	 * Set the {@link Schema} to a different instance.  This is typically called
	 * in special situations where the {@link MetadataFactory} logic is not used
	 * to construct the {@link Schema}.
	 * @param schema
	 */
	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	/**
	 * get runtime types keyed by runtime name, which is 
	 * a type name known to the Teiid engine
	 * @return
	 */
	public Map<String, Datatype> getDataTypes() {
		return dataTypes;
	}
	
	/**
	 * To be called if the MetadataFactory is deserialized to set the canonical system
	 * type value.
	 * @param dt
	 * @param builtin
	 */
	public void correctDatatypes(Map<String, Datatype> dt, Map<String, Datatype> builtin) {
		this.dataTypes = dt;
		this.builtinDataTypes = builtin;
		for (Table t : this.schema.getTables().values()) {
			correctDataTypes(t.getColumns());
		}
		for (Procedure p : this.schema.getProcedures().values()) {
			correctDataTypes(p.getParameters());
			if (p.getResultSet() != null) {
				correctDataTypes(p.getResultSet().getColumns());
			}
		}
	}

	private void correctDataTypes(List<? extends BaseColumn> cols) {
		if (cols == null) {
			return;
		}
		for (BaseColumn c : cols) {
			if (c.getDatatype() == null) {
				continue;
			}
			Datatype dt = this.builtinDataTypes.get(c.getDatatype().getName());
			if (dt != null) {
				c.setDatatype(dt);
			} else {
				//must be an enterprise type
				//if it's used in a single schema, we're ok, but when used in multiple there's an issue
				addDatatype(dt);
			}
		}
	}
	
	public void addDatatype(Datatype datatype) {
		this.builtinDataTypes.put(datatype.getName(), datatype);
	}

	public String getVdbName() {
		return vdbName;
	}

	public int getVdbVersion() {
		return vdbVersion;
	}	
	
	/**
	 * Get the namespace map.  Will be an unmodifiable empty map if {@link #addNamespace(String, String)}
	 * has not been called successfully.
	 * @return
	 */
	public Map<String, String> getNamespaces() {
		if (this.namespaces == null) {
			return Collections.emptyMap();
		}
		return namespaces;
	}
	
	public void setBuiltinDataTypes(Map<String, Datatype> builtinDataTypes) {
		this.builtinDataTypes = builtinDataTypes;
	}

	/**
	 * get all built-in types, known to Designer and defined in the system metadata.
	 * The entries are keyed by type name, which is typically the xsd type name. 
	 * @see #getDataTypes() for run-time types
	 * @return
	 */
	public Map<String, Datatype> getBuiltinDataTypes() {
		return builtinDataTypes;
	}
	
	/**
	 * Parses, but does not close, the given {@link Reader} into this {@link MetadataFactory}
	 * @param ddl
	 * @throws MetadataException
	 */
	public void parse(Reader ddl) throws MetadataException {
		this.parser.parseDDL(this, ddl);
	}
	
	public void setParser(Parser parser) {
		this.parser = parser;
	}
	
	public void setModel(ModelMetaData model) {
		this.model = model;
	}
	
	public Parser getParser() {
		return parser;
	}
}
