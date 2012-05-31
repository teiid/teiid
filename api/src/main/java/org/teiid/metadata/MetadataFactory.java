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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

import org.teiid.connector.DataPlugin;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;


/**
 * Allows connectors to build metadata for use by the engine.
 * 
 * TODO: add support for datatype import
 * TODO: add support for unique constraints
 */
public class MetadataFactory implements Serializable {
	private static final long serialVersionUID = 8590341087771685630L;
	
	private String vdbName;
	private int vdbVersion;
	private Map<String, Datatype> dataTypes;
	private boolean autoCorrectColumnNames = true;
	private Map<String, String> namespaces = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
	private String rawMetadata;
	private Properties importProperties;
	private Schema schema = new Schema();
	
	public MetadataFactory(String vdbName, int vdbVersion, String schemaName, Map<String, Datatype> dataTypes, Properties importProperties, String rawMetadata) {
		this.vdbName = vdbName;
		this.vdbVersion = vdbVersion;
		this.dataTypes = dataTypes;
		schema.setName(schemaName);
		setUUID(schema);	
		this.importProperties = importProperties;
		this.rawMetadata = rawMetadata;
	}
		
	public Properties getImportProperties() {
		return importProperties;
	}
	
	public String getRawMetadata() {
		return this.rawMetadata;
	}
		
	protected void setUUID(AbstractMetadataRecord record) {
		record.setUUID("mmuuid:" +UUID.randomUUID()); //$NON-NLS-1$
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
	 * @throws TranslatorException 
	 */
	public Table addTable(String name) throws TranslatorException {
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
	 * @throws TranslatorException
	 */
	public Column addColumn(String name, String type, ColumnSet<?> table) throws TranslatorException {
		if (autoCorrectColumnNames) {
			name.replace(AbstractMetadataRecord.NAME_DELIM_CHAR, '_');
		} else if (name.indexOf(AbstractMetadataRecord.NAME_DELIM_CHAR) != -1) {
			//TODO: for now this is not used
			 throw new TranslatorException(DataPlugin.Event.TEIID60008, DataPlugin.Util.gs(DataPlugin.Event.TEIID60008, name));
		}
		Column column = new Column();
		column.setName(name);
		table.addColumn(column);
		column.setPosition(table.getColumns().size()); //1 based indexing
		Datatype datatype = setColumnType(type, column);
		column.setCaseSensitive(datatype.isCaseSensitive());
		column.setAutoIncremented(datatype.isAutoIncrement());
		column.setSigned(datatype.isSigned());		
		setUUID(column);
		return column;
	}

	private Datatype setColumnType(String type,
			BaseColumn column) throws TranslatorException {
		Datatype datatype = dataTypes.get(type);
		if (datatype == null) {
			 throw new TranslatorException(DataPlugin.Event.TEIID60009, DataPlugin.Util.gs(DataPlugin.Event.TEIID60009, type));
		}
		column.setDatatype(datatype);
		column.setDatatypeUUID(datatype.getUUID());
		column.setLength(datatype.getLength());
		column.setPrecision(datatype.getPrecisionLength());
		column.setRadix(datatype.getRadix());
		column.setRuntimeType(datatype.getRuntimeTypeName());
		return datatype;
	}
	
	/**
	 * Adds a primary key to the given table.  The column names should be in key order.
	 * @param name
	 * @param columnNames
	 * @param table
	 * @return
	 * @throws TranslatorException
	 */
	public KeyRecord addPrimaryKey(String name, List<String> columnNames, Table table) throws TranslatorException {
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
	 * @throws TranslatorException
	 */
	public KeyRecord addAccessPattern(String name, List<String> columnNames, Table table) throws TranslatorException {
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
	 * @throws TranslatorException
	 */
	public KeyRecord addIndex(String name, boolean nonUnique, List<String> columnNames, Table table) throws TranslatorException {
		KeyRecord index = new KeyRecord(nonUnique?KeyRecord.Type.Index:KeyRecord.Type.Unique);
		index.setParent(table);
		index.setColumns(new ArrayList<Column>(columnNames.size()));
		index.setName(name);
		setUUID(index);
		assignColumns(columnNames, table, index);
		if (nonUnique) {
			table.getIndexes().add(index);
		}
		else {
			table.getUniqueKeys().add(index);
		}
		return index;
	}
	
	
	/**
	 * Adds a foreign key to the given table.  The referenced primary key must already exist.  The column names should be in key order.
	 * @param name
	 * @param columnNames
	 * @param referenceTable - schema qualified reference table name
	 * @param table
	 * @return
	 * @throws TranslatorException
	 */
	public ForeignKey addForiegnKey(String name, List<String> columnNames, String referenceTable, Table table) throws TranslatorException {
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
	 * @throws TranslatorException
	 */	
	public ForeignKey addForiegnKey(String name, List<String> columnNames, List<String> referencedColumnNames, String referenceTable, Table table) throws TranslatorException {
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
	 * @throws TranslatorException 
	 */
	public Procedure addProcedure(String name) throws TranslatorException {
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
	 * @throws TranslatorException 
	 */
	public ProcedureParameter addProcedureParameter(String name, String type, ProcedureParameter.Type parameterType, Procedure procedure) throws TranslatorException {
		ProcedureParameter param = new ProcedureParameter();
		param.setName(name);
		setUUID(param);
		param.setType(parameterType);
		param.setProcedure(procedure);
		setColumnType(type, param);
		procedure.getParameters().add(param);
		param.setPosition(procedure.getParameters().size()); //1 based indexing
		return param;
	}
	
	/**
	 * Add a procedure resultset column to the given procedure.
	 * @param name
	 * @param type should be one of {@link TypeFacility.RUNTIME_NAMES}
	 * @param procedure
	 * @return
	 * @throws TranslatorException 
	 */
	public Column addProcedureResultSetColumn(String name, String type, Procedure procedure) throws TranslatorException {
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
			ColumnSet<?> columns) throws TranslatorException {
		for (String columnName : columnNames) {
			boolean match = false;
			for (Column column : table.getColumns()) {
				if (column.getName().equals(columnName)) {
					match = true;
					columns.getColumns().add(column);
					break;
				}
			}
			if (!match) {
				 throw new TranslatorException(DataPlugin.Event.TEIID60011, DataPlugin.Util.gs(DataPlugin.Event.TEIID60011, columnName));
			}
		}
	}
	
	/**
	 * Add a function with the given name to the model.  
	 * @param name
	 * @return
	 * @throws TranslatorException 
	 */
	public FunctionMethod addFunction(String name) throws TranslatorException {
		FunctionMethod function = new FunctionMethod();
		function.setName(name);
		setUUID(function);
		schema.addFunction(function);
		return function;
	}
	
	/**
	 * Set to false to disable correcting column and other names to be valid Teiid names.
	 * @param autoCorrectColumnNames
	 */
	public void setAutoCorrectColumnNames(boolean autoCorrectColumnNames) {
		this.autoCorrectColumnNames = autoCorrectColumnNames;
	}
	
	public void addNamespace(String prefix, String uri) {
		this.namespaces.put(prefix, uri);
	}
	
	public void mergeInto (MetadataStore store) {
		store.addSchema(this.schema);
		store.addDataTypes(this.dataTypes.values());
		store.addNamespaces(this.namespaces);
	}
	
	public MetadataStore asMetadataStore() {
		MetadataStore store = new MetadataStore();
		mergeInto(store);
		return store;
	}
	
	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public Map<String, Datatype> getDataTypes() {
		return dataTypes;
	}
	
	void addDataTypes(Collection<Datatype> types) {
		for (Datatype type: types) {
			addDatatype(type);
		}
	}
	
	public void addDatatype(Datatype datatype) {
		if (this.dataTypes == null) {
			this.dataTypes = new TreeMap<String, Datatype>();
		}
		this.dataTypes.put(datatype.getName(), datatype);
	}

	public String getVdbName() {
		return vdbName;
	}

	public int getVdbVersion() {
		return vdbVersion;
	}	
	
	public Map<String, String> getNamespaces() {
		return namespaces;
	}
}
