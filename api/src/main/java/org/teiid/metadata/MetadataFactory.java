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

package org.teiid.metadata;

import java.io.Reader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.teiid.CommandContext;
import org.teiid.UserDefinedAggregate;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.connector.DataPlugin;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Database.ResourceType;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.TypeFacility;


/**
 * Allows connectors to build metadata for use by the engine.
 *
 * TODO: add support for datatype import
 * TODO: add support for unique constraints
 */
public class MetadataFactory extends NamespaceContainer {

	private static final long serialVersionUID = 8590341087771685630L;

	private String vdbName;
	private String vdbVersion;
	private Map<String, Datatype> dataTypes;
	private boolean autoCorrectColumnNames = true;
    private boolean renameDuplicateColumns;
	private String rawMetadata;
	private Properties modelProperties;
	private Schema schema = new Schema();
	private String idPrefix;
	protected int count;
	private transient Parser parser;
	private transient ModelMetaData model;
	private transient Map<String, ? extends VDBResource> vdbResources;
	private Map<String, Grant> grants;

	public MetadataFactory(String vdbName, Object vdbVersion, Map<String, Datatype> runtimeTypes, ModelMetaData model) {
		this(vdbName, vdbVersion, model.getName(), runtimeTypes, model.getProperties(), model.getSchemaText());
		this.model = model;
	}

	public MetadataFactory(String vdbName, Object vdbVersion, String schemaName, Map<String, Datatype> runtimeTypes, Properties modelProperties, String rawMetadata) {
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion.toString();
		this.dataTypes = Collections.unmodifiableMap(runtimeTypes);
		this.schema.setName(schemaName);
		long msb = longHash(vdbName, 0);
		try {
			//if this is just an int, we'll use the old style hash
			int val = Integer.parseInt(this.vdbVersion);
			msb = 31*msb + val;
		} catch (NumberFormatException e) {
			msb = 31*msb + vdbVersion.hashCode();
		}
		msb = longHash(schemaName, msb);
		this.idPrefix = "tid:" + hex(msb, 12); //$NON-NLS-1$
		setUUID(this.schema);
		if (modelProperties != null) {
			for (Map.Entry<Object, Object> entry : modelProperties.entrySet()) {
				if (entry.getKey() instanceof String && entry.getValue() instanceof String) {
					this.schema.setProperty(resolvePropertyKey(this, (String) entry.getKey()), (String) entry.getValue());
				}
			}
		    this.autoCorrectColumnNames = PropertiesUtils.getBooleanProperty(modelProperties, "importer.autoCorrectColumnNames", this.autoCorrectColumnNames); //$NON-NLS-1$
		    this.renameDuplicateColumns = PropertiesUtils.getBooleanProperty(modelProperties, "importer.renameDuplicateColumns", this.renameDuplicateColumns); //$NON-NLS-1$
		}
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

	public static String hex(long val, int hexLength) {
		long hi = 1L << (Math.min(63, hexLength * 4));
		return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }

	/**
	 * @deprecated
	 * @return
	 * @see #getModelProperties()
	 */
	@Deprecated
	public Properties getImportProperties() {
		return this.modelProperties;
	}

	public Properties getModelProperties() {
		return this.modelProperties;
	}

	/**
	 * Get the metadata text for the first metadata element
	 * @return
	 */
	@Deprecated
	public String getRawMetadata() {
		return this.rawMetadata;
	}

	public Model getModel() {
		return this.model;
	}

	protected void setUUID(AbstractMetadataRecord record) {
		int lsb = 0;
		if (record.getParent() != null) {
			lsb  = record.getParent().getUUID().hashCode();
		}
		lsb = 31*lsb + record.getName().hashCode();
		String uuid = this.idPrefix+"-"+hex(lsb, 8) + "-" + hex(this.count++, 8); //$NON-NLS-1$ //$NON-NLS-2$
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
		this.schema.addTable(table);
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
		if (this.autoCorrectColumnNames) {
			String newName = name.replace(AbstractMetadataRecord.NAME_DELIM_CHAR, '_');
			if (!newName.equals(name)) {
			    LogManager.logInfo(LogConstants.CTX_CONNECTOR, DataPlugin.Util.gs(DataPlugin.Event.TEIID60038, name, newName));
			}
			name = newName;
		} else if (name.indexOf(AbstractMetadataRecord.NAME_DELIM_CHAR) != -1) {
			throw new MetadataException(DataPlugin.Event.TEIID60008, DataPlugin.Util.gs(DataPlugin.Event.TEIID60008, table.getFullName(), name));
		}
		if (table.getColumnByName(name) != null) {
		    if (!renameDuplicateColumns) {
		        throw new DuplicateRecordException(DataPlugin.Event.TEIID60016, DataPlugin.Util.gs(DataPlugin.Event.TEIID60016, table.getFullName() + AbstractMetadataRecord.NAME_DELIM_CHAR + name));
		    }
		    int suffix = 1;
		    String newName = name + "_" + suffix; //$NON-NLS-1$
            while (table.getColumnByName(newName) != null) {
		        suffix++;
		    }
            LogManager.logInfo(LogConstants.CTX_CONNECTOR, DataPlugin.Util.gs(DataPlugin.Event.TEIID60039, newName, name));
            name = newName;
		}
		Column column = new Column();
		column.setName(name);
		table.addColumn(column);
		column.setParent(table);
		column.setPosition(table.getColumns().size()); //1 based indexing
		setDataType(type, column, this.dataTypes, false);
		setUUID(column);
		return column;
	}
	
    public Column renameColumn(String oldName, String name, ColumnSet<?> table) {
        if (this.autoCorrectColumnNames) {
            name = name.replace(AbstractMetadataRecord.NAME_DELIM_CHAR, '_');
        } else if (name.indexOf(AbstractMetadataRecord.NAME_DELIM_CHAR) != -1) {
            throw new MetadataException(DataPlugin.Event.TEIID60008, DataPlugin.Util.gs(DataPlugin.Event.TEIID60008, table.getFullName(), name));
        }
        if (table.getColumnByName(name) != null) {
            throw new DuplicateRecordException(DataPlugin.Event.TEIID60016, DataPlugin.Util.gs(DataPlugin.Event.TEIID60016, table.getFullName() + AbstractMetadataRecord.NAME_DELIM_CHAR + name));
        }
        Column column = table.getColumnByName(oldName);
        table.removeColumn(column);        
        column.setName(name);
        table.addColumn(column);
        return column;
    }	

	public static Datatype setDataType(String type, BaseColumn column, Map<String, Datatype> dataTypes, boolean allowNull) {
		int arrayDimensions = 0;
		while (DataTypeManager.isArrayType(type)) {
			arrayDimensions++;
			type = type.substring(0, type.length()-2);
		}
		Datatype datatype = dataTypes.get(type);
		if (datatype == null && (!allowNull || !DataTypeManager.DefaultDataTypes.NULL.equals(type))) {
			//TODO: potentially we want to check the enterprise types, but at
			//this point we're keying them by name, not runtime name (which
			// is an awkward difference to start with)
			//so the runtime type names are considered fixed and a type system
			//generalization would be needed to support injecting new runtime types
			 throw new MetadataException(DataPlugin.Event.TEIID60009, DataPlugin.Util.gs(DataPlugin.Event.TEIID60009, type));
		}
		column.setDatatype(datatype, true, arrayDimensions);
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
			throw new MetadataException(DataPlugin.Event.TEIID60011, DataPlugin.Util.gs(DataPlugin.Event.TEIID60011, table.getFullName(), columnName));
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
		this.schema.addProcedure(procedure);
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
		setDataType(type, param, this.dataTypes, false);
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
		this.schema.addFunction(function);
		return function;
	}

	/**
	 * Add a function with the given name to the model.  
	 * @param name
	 * @return
	 * @throws MetadataException 
	 */
	public FunctionMethod addFunction(String name, String returnType, String... paramTypes) {
		FunctionMethod function = FunctionMethod.createFunctionMethod(name, null, null, returnType, paramTypes);
		setFunctionMethodTypes(function);
		function.setPushdown(PushDown.MUST_PUSHDOWN);
		setUUID(function);
		schema.addFunction(function);
		return function;
	}

	private void setFunctionMethodTypes(FunctionMethod function) {
		FunctionParameter outputParameter = function.getOutputParameter();
		if (outputParameter != null) {
			setDataType(outputParameter.getRuntimeType(), outputParameter, dataTypes, outputParameter.getNullType() == NullType.Nullable);
		}
		for (FunctionParameter param : function.getInputParameters()) {
			setDataType(param.getRuntimeType(), param, dataTypes, param.getNullType() == NullType.Nullable);
		}
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
		setFunctionMethodTypes(func);
		setUUID(func);
		getSchema().addFunction(func);
		return func;
	}

	public static FunctionMethod createFunctionFromMethod(String name, Method method) {
		Class<?> returnTypeClass = method.getReturnType();
		AggregateAttributes aa = null;
		//handle user defined aggregates
		if ((method.getModifiers() & Modifier.STATIC) == 0 && UserDefinedAggregate.class.isAssignableFrom(method.getDeclaringClass())) {
			aa = new AggregateAttributes();
			Method m;
			try {
				m = method.getDeclaringClass().getMethod("getResult", CommandContext.class); //$NON-NLS-1$
			} catch (NoSuchMethodException e) {
				throw new TeiidRuntimeException(e);
			} catch (SecurityException e) {
				throw new TeiidRuntimeException(e);
			}
			returnTypeClass = m.getReturnType();
		}
		if (returnTypeClass.isPrimitive()) {
			returnTypeClass = TypeFacility.convertPrimitiveToObject(returnTypeClass);
		}
		String returnType = DataTypeManager.getDataTypeName(returnTypeClass);
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
		func.setAggregateAttributes(aa);
		func.setInvocationMethod(method.getName());
		func.setPushdown(PushDown.CAN_PUSHDOWN);
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

	public void mergeInto (MetadataStore store) {
		store.addSchema(this.schema);
		store.addDataTypes(this.dataTypes);
		if (this.grants != null) {
		    store.addGrants(this.grants.values());
		}
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
		return this.dataTypes;
	}

	/**
	 * To be called if the MetadataFactory is deserialized to set the canonical system
	 * type value.
	 * @param dt
	 * @param builtin
	 */
	public void correctDatatypes(Map<String, Datatype> dt) {
		this.dataTypes = dt;
		for (Table t : this.schema.getTables().values()) {
			correctDataTypes(t.getColumns());
		}
		for (Procedure p : this.schema.getProcedures().values()) {
			correctDataTypes(p.getParameters());
			if (p.getResultSet() != null) {
				correctDataTypes(p.getResultSet().getColumns());
			}
		}
		for (FunctionMethod p : this.schema.getFunctions().values()) {
			correctDataTypes(p.getInputParameters());
		}
	}

	private void correctDataTypes(List<? extends BaseColumn> cols) {
		if (cols == null) {
			return;
		}
		for (BaseColumn c : cols) {
			Datatype datatype = c.getDatatype();
			String name = null;
			if (datatype == null) {
				name = c.getRuntimeType();
			} else {
				name = datatype.getName();
			}
			Datatype dt = this.dataTypes.get(name);
			if (dt != null) {
				c.setDatatype(dt, false, c.getArrayDimensions());
			}
		}
	}

	public String getVdbName() {
		return this.vdbName;
	}

	public String getVdbVersion() {
		return this.vdbVersion;
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
		return this.namespaces;
	}

	/**
	 * Parses, but does not close, the given {@link Reader} into this {@link MetadataFactory}
	 * @param ddl
	 * @throws MetadataException
	 */
	public void parse(Reader ddl) throws MetadataException {
		this.parser.parseDDL(this, ddl);
        HashSet<FunctionMethod> functions = new HashSet<FunctionMethod>();
        for (FunctionMethod functionMethod : getSchema().getFunctions().values()) {
            if (!functions.add(functionMethod)) {
                throw new DuplicateRecordException(DataPlugin.Event.TEIID60015, 
                        DataPlugin.Util.gs(DataPlugin.Event.TEIID60015, functionMethod.getName()));
            }
        }		
	}

	public void setParser(Parser parser) {
		this.parser = parser;
	}

	public void setModel(ModelMetaData model) {
		this.model = model;
	}

	public Parser getParser() {
		return this.parser;
	}

	public Map<String, ? extends VDBResource> getVDBResources() {
		return this.vdbResources;
	}

	public void setVdbResources(Map<String, ? extends VDBResource> vdbResources) {
		this.vdbResources = vdbResources;
	}

	/**
	 * Add a permission for a {@link Table} or {@link Procedure}
	 * @param role
	 * @param resource
	 * @param allowAlter
	 * @param allowCreate
	 * @param allowRead
	 * @param allowUpdate
	 * @param allowDelete
	 * @param allowExecute
	 * @param condition
	 * @param constraint
	 */
	public void addPermission(String role, AbstractMetadataRecord resource, Boolean allowAlter, Boolean allowCreate, Boolean allowRead, Boolean allowUpdate,
			Boolean allowDelete, Boolean allowExecute, String condition, Boolean constraint) {
		Grant.Permission pmd = new Grant.Permission();
		pmd.setResourceName(resource.getFullName());
		if (resource instanceof Table) {
		    pmd.setResourceType(ResourceType.TABLE);
		} else {
		    pmd.setResourceType(ResourceType.PROCEDURE);
		}
		pmd.setAllowAlter(allowAlter);
		pmd.setAllowInsert(allowCreate);
		pmd.setAllowDelete(allowDelete);
		pmd.setAllowExecute(allowExecute);
		pmd.setAllowSelect(allowRead);
		pmd.setAllowUpdate(allowUpdate);
		pmd.setCondition(condition, constraint);
		addPermission(pmd, role);
	}

	/**
	 * Add a permission for the current {@link Schema} which will typically act as a default for all child objects.
	 * @param role
	 * @param allowAlter
	 * @param allowCreate
	 * @param allowRead
	 * @param allowUpdate
	 * @param allowDelete
	 * @param allowExecute
	 */
	public void addSchemaPermission(String role, Boolean allowAlter, Boolean allowCreate, Boolean allowRead, Boolean allowUpdate,
			Boolean allowDelete, Boolean allowExecute) {
        Grant.Permission pmd = new Grant.Permission();
        pmd.setResourceName(this.schema.getFullName());
        pmd.setResourceType(ResourceType.SCHEMA);
		
		pmd.setAllowAlter(allowAlter);
		pmd.setAllowInsert(allowCreate);
		pmd.setAllowDelete(allowDelete);
		pmd.setAllowExecute(allowExecute);
		pmd.setAllowSelect(allowRead);
		pmd.setAllowUpdate(allowUpdate);
		addPermission(pmd, role);
	}

	/**
	 * Add a permission for a {@link Column}
	 * @param role
	 * @param resource
	 * @param allowCreate
	 * @param allowRead
	 * @param allowUpdate
	 * @param condition
	 * @param mask
	 * @param order
	 */
	public void addColumnPermission(String role, Column resource, Boolean allowCreate, Boolean allowRead, Boolean allowUpdate,
			String condition, String mask, Integer order) {
        Grant.Permission pmd = new Grant.Permission();
        pmd.setResourceType(ResourceType.COLUMN);
	    
        String resourceName = null;
    	if (resource.getParent() != null && resource.getParent().getParent() instanceof Procedure) {
    		resourceName = resource.getParent().getParent().getFullName() + '.' + resource.getName();
    	} else {
    		resourceName = resource.getFullName();
    	}
		pmd.setResourceName(resourceName);
		pmd.setAllowInsert(allowCreate);
		pmd.setAllowSelect(allowRead);
		pmd.setAllowUpdate(allowUpdate);
		pmd.setCondition(condition, null);
		pmd.setMask(mask);
		pmd.setMaskOrder(order);
		addPermission(pmd, role);
	}

	private void addPermission(Grant.Permission pmd, String role) {
		if (this.grants == null) {
			this.grants = new TreeMap<String, Grant>();
		}
		Grant g = this.grants.get(role);
		if (g == null) {
		    g = new Grant();
		    g.setRole(role);
		    this.grants.put(role, g);
		}
		g.addPermission(pmd);
	}

	public void addFunction(FunctionMethod functionMethod) {
		functionMethod.setParent(this.schema);
		setUUID(functionMethod);
		for (FunctionParameter param : functionMethod.getInputParameters()) {
			setUUID(param);
		}
		setUUID(functionMethod.getOutputParameter());
		this.schema.addFunction(functionMethod);
	}

}
