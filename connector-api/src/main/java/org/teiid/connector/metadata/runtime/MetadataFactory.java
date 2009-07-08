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
import java.util.Map;

import org.teiid.connector.DataPlugin;
import org.teiid.connector.api.ConnectorException;

import com.metamatrix.core.id.UUIDFactory;
import com.metamatrix.core.vdb.ModelType;

public class MetadataFactory implements ConnectorMetadata {
	
	private transient UUIDFactory factory = new UUIDFactory();
	private transient Map<String, DatatypeRecordImpl> dataTypes;
	
	private ModelRecordImpl model;
	private Collection<TableRecordImpl> tables = new ArrayList<TableRecordImpl>();
	private Collection<ProcedureRecordImpl> procedures = new ArrayList<ProcedureRecordImpl>();
	
	public MetadataFactory(String modelName, Map<String, DatatypeRecordImpl> dataTypes) {
		this.dataTypes = dataTypes;
		model = new ModelRecordImpl();
		model.setFullName(modelName);
		model.setModelType(ModelType.PHYSICAL);
		model.setRecordType('A');
		model.setPrimaryMetamodelUri("http://www.metamatrix.com/metamodels/Relational"); //$NON-NLS-1$
		setUUID(model);	
	}
	
	public ModelRecordImpl getModel() {
		return model;
	}
	
	public Collection<TableRecordImpl> getTables() {
		return tables;
	}
	
	public Collection<ProcedureRecordImpl> getProcedures() {
		return procedures;
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
		table.setRecordType('B');
		table.setCardinality(-1);
		table.setModel(model);
		table.setTableType(MetadataConstants.TABLE_TYPES.TABLE_TYPE);
		setUUID(table);
		this.tables.add(table);
		return table;
	}
	
	public ColumnRecordImpl addColumn(String name, String type, TableRecordImpl table) throws ConnectorException {
		ColumnRecordImpl column = new ColumnRecordImpl();
		setValuesUsingParent(name, table, column);
		column.setRecordType('G');
		if (table.getColumns() == null) {
			table.setColumns(new ArrayList<ColumnRecordImpl>());
		}
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
		setUUID(column);
		return column;
	}
	
}
