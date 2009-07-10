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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.connector.DataPlugin;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.metadata.runtime.MetadataConstants.RECORD_TYPE;

import com.metamatrix.core.id.UUIDFactory;
import com.metamatrix.core.vdb.ModelType;

public class MetadataFactory implements ConnectorMetadata {
	
	private transient UUIDFactory factory = new UUIDFactory();
	private transient Map<String, DatatypeRecordImpl> dataTypes;
	
	private ModelRecordImpl model;
	private Collection<TableRecordImpl> tables = new ArrayList<TableRecordImpl>();
	private Collection<ProcedureRecordImpl> procedures = new ArrayList<ProcedureRecordImpl>();
	private Collection<AnnotationRecordImpl> annotations = new ArrayList<AnnotationRecordImpl>();
	private Collection<PropertyRecordImpl> properties = new ArrayList<PropertyRecordImpl>();
	
	public MetadataFactory(String modelName, Map<String, DatatypeRecordImpl> dataTypes) {
		this.dataTypes = dataTypes;
		model = new ModelRecordImpl();
		model.setFullName(modelName);
		model.setModelType(ModelType.PHYSICAL);
		model.setRecordType(RECORD_TYPE.MODEL);
		model.setPrimaryMetamodelUri("http://www.metamatrix.com/metamodels/Relational"); //$NON-NLS-1$
		setUUID(model);	
	}
	
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
	
	private static void setValuesUsingParent(String name,
			AbstractMetadataRecord parent, AbstractMetadataRecord child) {
		child.setFullName(parent.getFullName() + "." + name); //$NON-NLS-1$
		child.setParentUUID(parent.getUUID());
	}
	
	public TableRecordImpl addTable(String name) {
		TableRecordImpl table = new TableRecordImpl();
		setValuesUsingParent(name, model, table);
		table.setRecordType(RECORD_TYPE.TABLE);
		table.setModel(model);
		table.setTableType(MetadataConstants.TABLE_TYPES.TABLE_TYPE);
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
	
	public ColumnRecordImpl addColumn(String name, String type, TableRecordImpl table) throws ConnectorException {
		ColumnRecordImpl column = new ColumnRecordImpl();
		setValuesUsingParent(name, table, column);
		column.setRecordType(RECORD_TYPE.COLUMN);
		table.getColumns().add(column);
		column.setPosition(table.getColumns().size());
		column.setNullValues(-1);
		column.setDistinctValues(-1);
		DatatypeRecordImpl datatype = dataTypes.get(type);
		if (datatype == null) {
			throw new ConnectorException(DataPlugin.Util.getString("MetadataFactory.unknown_datatype", type)); //$NON-NLS-1$
		}
		column.setDatatype(datatype);
		column.setCaseSensitive(datatype.isCaseSensitive());
		column.setAutoIncrementable(datatype.isAutoIncrement());
		column.setDatatypeUUID(datatype.getUUID());
		column.setLength(datatype.getLength());
		column.setNullType(datatype.getNullType());
		column.setPrecision(datatype.getPrecisionLength());
		column.setRadix(datatype.getRadix());
		column.setRuntimeType(datatype.getRuntimeTypeName());
		column.setSelectable(true);
		column.setSigned(datatype.isSigned());
		column.setExtensionProperties(new LinkedList<PropertyRecordImpl>());
		setUUID(column);
		return column;
	}
	
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
	
	public ColumnSetRecordImpl addPrimaryKey(String name, List<String> columnNames, TableRecordImpl table) throws ConnectorException {
		ColumnSetRecordImpl primaryKey = new ColumnSetRecordImpl(MetadataConstants.KEY_TYPES.PRIMARY_KEY);
		primaryKey.setColumns(new ArrayList<ColumnRecordImpl>(columnNames.size()));
		primaryKey.setRecordType(RECORD_TYPE.PRIMARY_KEY);
		setUUID(primaryKey);
		setValuesUsingParent(name, table, primaryKey);
		assignColumns(columnNames, table, primaryKey);
		table.setPrimaryKey(primaryKey);
		table.setPrimaryKeyID(primaryKey.getUUID());
		return primaryKey;
	}
	
	public ColumnSetRecordImpl addIndex(String name, boolean nonUnique, List<String> columnNames, TableRecordImpl table) throws ConnectorException {
		ColumnSetRecordImpl index = new ColumnSetRecordImpl(nonUnique?MetadataConstants.KEY_TYPES.INDEX:MetadataConstants.KEY_TYPES.UNIQUE_KEY);
		index.setColumns(new ArrayList<ColumnRecordImpl>(columnNames.size()));
		index.setRecordType(nonUnique?MetadataConstants.RECORD_TYPE.INDEX:MetadataConstants.RECORD_TYPE.UNIQUE_KEY);
		setUUID(index);
		setValuesUsingParent(name, table, index);
		assignColumns(columnNames, table, index);
		if (nonUnique) {
			table.getIndexes().add(index);
		} else {
			table.getUniqueKeys().add(index);
		}
		return index;
	}
	
	public ForeignKeyRecordImpl addForiegnKey(String name, List<String> columnNames, TableRecordImpl pkTable, TableRecordImpl table) throws ConnectorException {
		ForeignKeyRecordImpl foreignKey = new ForeignKeyRecordImpl();
		foreignKey.setColumns(new ArrayList<ColumnRecordImpl>(columnNames.size()));
		foreignKey.setRecordType(RECORD_TYPE.FOREIGN_KEY);
		setUUID(foreignKey);
		setValuesUsingParent(name, table, foreignKey);
		foreignKey.setPrimaryKey(pkTable.getPrimaryKey());
		foreignKey.setUniqueKeyID(pkTable.getPrimaryKeyID());
		assignColumns(columnNames, table, foreignKey);
		table.getForeignKeys().add(foreignKey);
		return foreignKey;
	}
	
	public PropertyRecordImpl addExtensionProperty(String name, String value, AbstractMetadataRecord record) {
		PropertyRecordImpl property = new PropertyRecordImpl();
		property.setRecordType(RECORD_TYPE.PROPERTY);
		setUUID(property);
		setValuesUsingParent(name, record, property);
		property.setPropertyName(name);
		property.setPropertyValue(value);
		properties.add(property);
		if (record.getExtensionProperties() == null) {
			record.setExtensionProperties(new LinkedList<PropertyRecordImpl>());
		}
		record.getExtensionProperties().add(property);
		return property;
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
				throw new ConnectorException("No column found with name " + columnName);
			}
		}
	}
		
}
