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

package org.teiid.connector.metadata.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.teiid.connector.DataPlugin;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.metadata.runtime.MetadataConstants.PARAMETER_TYPES;
import org.teiid.connector.metadata.runtime.MetadataConstants.RECORD_TYPE;

import com.metamatrix.core.id.UUIDFactory;
import com.metamatrix.core.vdb.ModelType;

public class MetadataFactory implements ConnectorMetadata {
	
	private transient UUIDFactory factory = new UUIDFactory();
	private transient Map<String, DatatypeRecordImpl> dataTypes;
	private transient Properties importProperties;
	
	private ModelRecordImpl model;
	private Collection<TableRecordImpl> tables = new ArrayList<TableRecordImpl>();
	private Collection<ProcedureRecordImpl> procedures = new ArrayList<ProcedureRecordImpl>();
	private Collection<AnnotationRecordImpl> annotations = new ArrayList<AnnotationRecordImpl>();
	private Collection<PropertyRecordImpl> properties = new ArrayList<PropertyRecordImpl>();
	
	private Set<String> uniqueNames = new HashSet<String>();
	
	public MetadataFactory(String modelName, Map<String, DatatypeRecordImpl> dataTypes, Properties importProperties) {
		this.dataTypes = dataTypes;
		this.importProperties = importProperties;
		model = new ModelRecordImpl();
		model.setFullName(modelName);
		model.setModelType(ModelType.PHYSICAL);
		model.setRecordType(RECORD_TYPE.MODEL);
		model.setPrimaryMetamodelUri("http://www.metamatrix.com/metamodels/Relational"); //$NON-NLS-1$
		setUUID(model);	
	}
	
	public Properties getImportProperties() {
		return importProperties;
	}
	
	@Override
	public ModelRecordImpl getModel() {
		return model;
	}
	
	@Override
	public Collection<TableRecordImpl> getTables() {
		return tables;
	}
	
	@Override
	public Collection<ProcedureRecordImpl> getProcedures() {
		return procedures;
	}
	
	@Override
	public Collection<AnnotationRecordImpl> getAnnotations() {
		return annotations;
	}
	
	public Collection<PropertyRecordImpl> getProperties() {
		return properties;
	}
	
	private void setUUID(AbstractMetadataRecord record) {
		record.setUUID(factory.create().toString());
	}

	private void setValuesUsingParent(String name,
			AbstractMetadataRecord parent, AbstractMetadataRecord child) throws ConnectorException {
		child.setFullName(parent.getFullName() + "." + name); //$NON-NLS-1$
		child.setParentUUID(parent.getUUID());
		if (!uniqueNames.add(child.getRecordType() + "/" + child.getFullName())) { //$NON-NLS-1$
			throw new ConnectorException(DataPlugin.Util.getString("MetadataFactory.duplicate_name", child)); //$NON-NLS-1$
		}
	}
	
	/**
	 * Add a table with the given name to the model.  
	 * @param name
	 * @return
	 * @throws ConnectorException 
	 */
	public TableRecordImpl addTable(String name) throws ConnectorException {
		TableRecordImpl table = new TableRecordImpl();
		setValuesUsingParent(name, model, table);
		table.setRecordType(RECORD_TYPE.TABLE);
		table.setColumns(new LinkedList<ColumnRecordImpl>());
		table.setAccessPatterns(new LinkedList<ColumnSetRecordImpl>());
		table.setIndexes(new LinkedList<ColumnSetRecordImpl>());
		table.setExtensionProperties(new LinkedList<PropertyRecordImpl>());
		table.setForiegnKeys(new LinkedList<ForeignKeyRecordImpl>());
		table.setUniqueKeys(new LinkedList<ColumnSetRecordImpl>());
		setUUID(table);
		this.tables.add(table);
		return table;
	}
	
	/**
	 * Adds a column to the table with the given name and type.
	 * @param name
	 * @param type should be one of {@link TypeFacility.RUNTIME_NAMES}
	 * @param table
	 * @return
	 * @throws ConnectorException
	 */
	public ColumnRecordImpl addColumn(String name, String type, ColumnSetRecordImpl table) throws ConnectorException {
		ColumnRecordImpl column = new ColumnRecordImpl();
		setValuesUsingParent(name, table, column);
		column.setRecordType(RECORD_TYPE.COLUMN);
		table.getColumns().add(column);
		column.setPosition(table.getColumns().size()); //1 based indexing
		DatatypeRecordImpl datatype = setColumnType(type, column);
		column.setCaseSensitive(datatype.isCaseSensitive());
		column.setAutoIncrementable(datatype.isAutoIncrement());
		column.setSigned(datatype.isSigned());		
		column.setExtensionProperties(new LinkedList<PropertyRecordImpl>());
		setUUID(column);
		return column;
	}

	private DatatypeRecordImpl setColumnType(String type,
			BaseColumn column) throws ConnectorException {
		DatatypeRecordImpl datatype = dataTypes.get(type);
		if (datatype == null) {
			throw new ConnectorException(DataPlugin.Util.getString("MetadataFactory.unknown_datatype", type)); //$NON-NLS-1$
		}
		column.setDatatype(datatype);
		column.setDatatypeUUID(datatype.getUUID());
		column.setLength(datatype.getLength());
		column.setNullType(datatype.getNullType());
		column.setPrecision(datatype.getPrecisionLength());
		column.setRadix(datatype.getRadix());
		column.setRuntimeType(datatype.getRuntimeTypeName());
		return datatype;
	}
	
	/**
	 * Add an annotation of description to a record.  Only one annotation should be added per record.
	 * @param annotation
	 * @param record
	 * @return
	 */
	public AnnotationRecordImpl addAnnotation(String annotation, AbstractMetadataRecord record) {
		AnnotationRecordImpl annotationRecordImpl = new AnnotationRecordImpl();
		annotationRecordImpl.setRecordType(RECORD_TYPE.ANNOTATION);
		setUUID(annotationRecordImpl);
		annotationRecordImpl.setParentUUID(record.getUUID());
		annotationRecordImpl.setDescription(annotation);
		record.setAnnotation(annotationRecordImpl);
		annotations.add(annotationRecordImpl);
		return annotationRecordImpl;
	}
	
	/**
	 * Adds a primary key to the given table.  The column names should be in key order.
	 * @param name
	 * @param columnNames
	 * @param table
	 * @return
	 * @throws ConnectorException
	 */
	public ColumnSetRecordImpl addPrimaryKey(String name, List<String> columnNames, TableRecordImpl table) throws ConnectorException {
		ColumnSetRecordImpl primaryKey = new ColumnSetRecordImpl(MetadataConstants.KEY_TYPES.PRIMARY_KEY);
		primaryKey.setColumns(new ArrayList<ColumnRecordImpl>(columnNames.size()));
		primaryKey.setRecordType(RECORD_TYPE.PRIMARY_KEY);
		setValuesUsingParent(name, table, primaryKey);
		setUUID(primaryKey);
		assignColumns(columnNames, table, primaryKey);
		table.setPrimaryKey(primaryKey);
		table.setPrimaryKeyID(primaryKey.getUUID());
		return primaryKey;
	}
	
	/**
	 * Adds an index or unique key constraint to the given table.
	 * @param name
	 * @param nonUnique true indicates that an index is being added.
	 * @param columnNames
	 * @param table
	 * @return
	 * @throws ConnectorException
	 */
	public ColumnSetRecordImpl addIndex(String name, boolean nonUnique, List<String> columnNames, TableRecordImpl table) throws ConnectorException {
		ColumnSetRecordImpl index = new ColumnSetRecordImpl(nonUnique?MetadataConstants.KEY_TYPES.INDEX:MetadataConstants.KEY_TYPES.UNIQUE_KEY);
		index.setColumns(new ArrayList<ColumnRecordImpl>(columnNames.size()));
		index.setRecordType(nonUnique?MetadataConstants.RECORD_TYPE.INDEX:MetadataConstants.RECORD_TYPE.UNIQUE_KEY);
		setValuesUsingParent(name, table, index);
		setUUID(index);
		assignColumns(columnNames, table, index);
		if (nonUnique) {
			table.getIndexes().add(index);
		} else {
			table.getUniqueKeys().add(index);
		}
		return index;
	}
	
	/**
	 * Adds a foreign key to the given table.  The column names should be in key order.
	 * @param name
	 * @param columnNames
	 * @param pkTable
	 * @param table
	 * @return
	 * @throws ConnectorException
	 */
	public ForeignKeyRecordImpl addForiegnKey(String name, List<String> columnNames, TableRecordImpl pkTable, TableRecordImpl table) throws ConnectorException {
		ForeignKeyRecordImpl foreignKey = new ForeignKeyRecordImpl();
		foreignKey.setColumns(new ArrayList<ColumnRecordImpl>(columnNames.size()));
		foreignKey.setRecordType(RECORD_TYPE.FOREIGN_KEY);
		setValuesUsingParent(name, table, foreignKey);
		setUUID(foreignKey);
		foreignKey.setPrimaryKey(pkTable.getPrimaryKey());
		foreignKey.setUniqueKeyID(pkTable.getPrimaryKeyID());
		assignColumns(columnNames, table, foreignKey);
		table.getForeignKeys().add(foreignKey);
		return foreignKey;
	}
	
	/**
	 * Adds an extension property to the given record.
	 * @param name
	 * @param value
	 * @param record
	 * @return
	 * @throws ConnectorException 
	 */
	public PropertyRecordImpl addExtensionProperty(String name, String value, AbstractMetadataRecord record) throws ConnectorException {
		PropertyRecordImpl property = new PropertyRecordImpl();
		property.setRecordType(RECORD_TYPE.PROPERTY);
		setValuesUsingParent(name, record, property);
		setUUID(property);
		property.setPropertyName(name);
		property.setPropertyValue(value);
		properties.add(property);
		if (record.getExtensionProperties() == null) {
			record.setExtensionProperties(new LinkedList<PropertyRecordImpl>());
		}
		record.getExtensionProperties().add(property);
		return property;
	}
	
	/**
	 * Add a procedure with the given name to the model.  
	 * @param name
	 * @return
	 * @throws ConnectorException 
	 */
	public ProcedureRecordImpl addProcedure(String name) throws ConnectorException {
		ProcedureRecordImpl procedure = new ProcedureRecordImpl();
		procedure.setRecordType(RECORD_TYPE.CALLABLE);
		setValuesUsingParent(name, this.model, procedure);
		setUUID(procedure);
		procedure.setParameters(new LinkedList<ProcedureParameterRecordImpl>());
		this.procedures.add(procedure);
		return procedure;
	}
	
	/**
	 * 
	 * @param name
	 * @param type should be one of {@link PARAMETER_TYPES}
	 * @param parameterType should be one of {@link TypeFacility.RUNTIME_NAMES}
	 * @param procedure
	 * @return
	 * @throws ConnectorException 
	 */
	public ProcedureParameterRecordImpl addProcedureParameter(String name, String type, short parameterType, ProcedureRecordImpl procedure) throws ConnectorException {
		ProcedureParameterRecordImpl param = new ProcedureParameterRecordImpl();
		param.setRecordType(RECORD_TYPE.CALLABLE_PARAMETER);
		setValuesUsingParent(name, procedure, param);
		setUUID(param);
		param.setType(parameterType);
		setColumnType(type, param);
		procedure.getParameters().add(param);
		param.setPosition(procedure.getParameters().size()); //1 based indexing
		return param;
	}
	
	/**
	 * 
	 * @param name
	 * @param type should be one of {@link TypeFacility.RUNTIME_NAMES}
	 * @param procedure
	 * @return
	 * @throws ConnectorException 
	 */
	public ColumnRecordImpl addProcedureResultSetColumn(String name, String type, ProcedureRecordImpl procedure) throws ConnectorException {
		if (procedure.getResultSet() == null) {
			ColumnSetRecordImpl resultSet = new ColumnSetRecordImpl((short)-1);
			resultSet.setRecordType(RECORD_TYPE.RESULT_SET);
			setValuesUsingParent("RESULT_SET", procedure, resultSet); //$NON-NLS-1$
			setUUID(resultSet);
			procedure.setResultSet(resultSet);
			procedure.setResultSetID(resultSet.getUUID());
		}
		return addColumn(name, type, procedure.getResultSet());
	}

	private void assignColumns(List<String> columnNames, TableRecordImpl table,
			ColumnSetRecordImpl columns) throws ConnectorException {
		for (String columnName : columnNames) {
			boolean match = false;
			for (ColumnRecordImpl column : table.getColumns()) {
				if (column.getName().equals(columnName)) {
					match = true;
					columns.getColumns().add(column);
					break;
				}
			}
			if (!match) {
				throw new ConnectorException(DataPlugin.Util.getString("MetadataFactory.no_column_found", columnName)); //$NON-NLS-1$
			}
		}
	}
		
}
