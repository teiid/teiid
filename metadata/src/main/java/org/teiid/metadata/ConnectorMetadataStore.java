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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.ColumnRecordImpl;
import org.teiid.connector.metadata.runtime.ColumnSetRecordImpl;
import org.teiid.connector.metadata.runtime.ConnectorMetadata;
import org.teiid.connector.metadata.runtime.MetadataConstants;
import org.teiid.connector.metadata.runtime.ModelRecordImpl;
import org.teiid.connector.metadata.runtime.ProcedureParameterRecordImpl;
import org.teiid.connector.metadata.runtime.ProcedureRecordImpl;
import org.teiid.connector.metadata.runtime.PropertyRecordImpl;
import org.teiid.connector.metadata.runtime.TableRecordImpl;
import org.teiid.metadata.index.IndexConstants;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.query.metadata.MetadataStore;

public class ConnectorMetadataStore implements MetadataStore {
	
	private ConnectorMetadata metadata;
	private String modelName;
	
	public ConnectorMetadataStore(String modelName, ConnectorMetadata metadata) {
		this.modelName = modelName;
		this.metadata = metadata;
	}
	
	@Override
	public ModelRecordImpl getModel(String fullName)
			throws QueryMetadataException, MetaMatrixComponentException {
		//there's no need to check the name, the CompositeMetadataStore will have already done that
		return metadata.getModel();
	}
	
	@Override
	public ColumnRecordImpl findElement(String fullName)
			throws QueryMetadataException, MetaMatrixComponentException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public TableRecordImpl findGroup(String fullName)
			throws QueryMetadataException, MetaMatrixComponentException {
		for (TableRecordImpl tableRecordImpl : metadata.getTables()) {
			if (tableRecordImpl.getFullName().equalsIgnoreCase(fullName)) {
				return tableRecordImpl;
			}
		}
        throw new QueryMetadataException(fullName+TransformationMetadata.NOT_EXISTS_MESSAGE);
	}
	
	@Override
	public Collection<String> getGroupsForPartialName(String partialGroupName)
			throws MetaMatrixComponentException, QueryMetadataException {
		Collection<String> results = new LinkedList<String>();
		for (TableRecordImpl tableRecordImpl : metadata.getTables()) {
			if (tableRecordImpl.getFullName().toLowerCase().endsWith(partialGroupName)) {
				results.add(tableRecordImpl.getFullName());
			}
		}
		return results;
	}
	
	@Override
	public Collection<String> getModelNames() {
		return Arrays.asList(modelName);
	}
	
	@Override
	public ProcedureRecordImpl getStoredProcedure(
			String fullyQualifiedProcedureName)
			throws MetaMatrixComponentException, QueryMetadataException {
		for (ProcedureRecordImpl procedureRecordImpl : metadata.getProcedures()) {
			if (procedureRecordImpl.getFullName().equalsIgnoreCase(fullyQualifiedProcedureName)) {
				return procedureRecordImpl;
			}
		}
        throw new QueryMetadataException(fullyQualifiedProcedureName+TransformationMetadata.NOT_EXISTS_MESSAGE);
	}
	
	@Override
	public Collection getXMLTempGroups(TableRecordImpl table)
			throws MetaMatrixComponentException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<? extends AbstractMetadataRecord> findMetadataRecords(char recordType,
			String entityName, boolean isPartialName)
			throws MetaMatrixComponentException {
		throw new UnsupportedOperationException();
	}

	private Collection<? extends AbstractMetadataRecord> getRecordsByType(
			char recordType) {
		switch (recordType) {
		case MetadataConstants.RECORD_TYPE.CALLABLE:
			return metadata.getProcedures();
		case MetadataConstants.RECORD_TYPE.CALLABLE_PARAMETER: {
			Collection<ProcedureParameterRecordImpl> results = new ArrayList<ProcedureParameterRecordImpl>();
			for (ProcedureRecordImpl procedure : metadata.getProcedures()) {
				results.addAll(procedure.getParameters());
			}
			return results;
		}
		case MetadataConstants.RECORD_TYPE.RESULT_SET: {
			Collection<ColumnSetRecordImpl> results = new ArrayList<ColumnSetRecordImpl>();
			for (ProcedureRecordImpl procedure : metadata.getProcedures()) {
				if (procedure.getResultSet() != null) {
					results.add(procedure.getResultSet());
				}
			}
			return results;
		}
		case MetadataConstants.RECORD_TYPE.ACCESS_PATTERN: {
			Collection<ColumnSetRecordImpl> results = new ArrayList<ColumnSetRecordImpl>();
			for (TableRecordImpl table : metadata.getTables()) {
				results.addAll(table.getAccessPatterns());
			}
			return results;
		}
		case MetadataConstants.RECORD_TYPE.UNIQUE_KEY: {
			Collection<ColumnSetRecordImpl> results = new ArrayList<ColumnSetRecordImpl>();
			for (TableRecordImpl table : metadata.getTables()) {
				results.addAll(table.getUniqueKeys());
			}
			return results;
		}
		case MetadataConstants.RECORD_TYPE.PRIMARY_KEY: {
			Collection<ColumnSetRecordImpl> results = new ArrayList<ColumnSetRecordImpl>();
			for (TableRecordImpl table : metadata.getTables()) {
				if (table.getPrimaryKey() != null) {
					results.add(table.getPrimaryKey());
				}
			}
			return results;
		}
		case MetadataConstants.RECORD_TYPE.FOREIGN_KEY: {
			Collection<ColumnSetRecordImpl> results = new ArrayList<ColumnSetRecordImpl>();
			for (TableRecordImpl table : metadata.getTables()) {
				results.addAll(table.getForeignKeys());
			}
			return results;
		}
		case MetadataConstants.RECORD_TYPE.INDEX: {
			Collection<ColumnSetRecordImpl> results = new ArrayList<ColumnSetRecordImpl>();
			for (TableRecordImpl table : metadata.getTables()) {
				results.addAll(table.getIndexes());
			}
			return results;
		}
		case MetadataConstants.RECORD_TYPE.MODEL:
			return Arrays.asList(metadata.getModel());
		case MetadataConstants.RECORD_TYPE.TABLE:
			return metadata.getTables();
		case MetadataConstants.RECORD_TYPE.COLUMN: {
			Collection<ColumnRecordImpl> results = new ArrayList<ColumnRecordImpl>();
			for (TableRecordImpl table : metadata.getTables()) {
				results.addAll(table.getColumns());
			}
			return results;
		}
		case MetadataConstants.RECORD_TYPE.ANNOTATION: {
			return metadata.getAnnotations();
		}
		case MetadataConstants.RECORD_TYPE.PROPERTY: {
			return metadata.getProperties();
		}
		}
		return Collections.emptyList();
	}

	@Override
	public Collection<? extends AbstractMetadataRecord> findMetadataRecords(String indexName,
			String pattern, boolean isPrefix,
			boolean isCaseSensitive) throws MetaMatrixCoreException {
		if (indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.COLUMNS_INDEX)) {
			return getRecordsByType(MetadataConstants.RECORD_TYPE.COLUMN);
		} else if (indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.KEYS_INDEX)) {
			List<AbstractMetadataRecord> result = new ArrayList<AbstractMetadataRecord>();
			result.addAll(getRecordsByType(MetadataConstants.RECORD_TYPE.ACCESS_PATTERN));
			result.addAll(getRecordsByType(MetadataConstants.RECORD_TYPE.UNIQUE_KEY));
			result.addAll(getRecordsByType(MetadataConstants.RECORD_TYPE.PRIMARY_KEY));
			result.addAll(getRecordsByType(MetadataConstants.RECORD_TYPE.FOREIGN_KEY));
			result.addAll(getRecordsByType(MetadataConstants.RECORD_TYPE.INDEX));
			return result;
		} else if (indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.MODELS_INDEX)) {
			return getRecordsByType(MetadataConstants.RECORD_TYPE.MODEL);
		} else if (indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.PROCEDURES_INDEX)) {
			List<AbstractMetadataRecord> result = new ArrayList<AbstractMetadataRecord>();
			result.addAll(getRecordsByType(MetadataConstants.RECORD_TYPE.CALLABLE));
			result.addAll(getRecordsByType(MetadataConstants.RECORD_TYPE.CALLABLE_PARAMETER));
			result.addAll(getRecordsByType(MetadataConstants.RECORD_TYPE.RESULT_SET));
			return result;
		} else if (indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.TABLES_INDEX)) {
			return getRecordsByType(MetadataConstants.RECORD_TYPE.TABLE);
		} else if (indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.ANNOTATION_INDEX)) {
			return getRecordsByType(MetadataConstants.RECORD_TYPE.ANNOTATION);
		}
		return Collections.emptyList();
	}
	
	@Override
	public boolean postProcessFindMetadataRecords() {
		return true;
	}

	@Override
	public Collection<PropertyRecordImpl> getExtensionProperties(AbstractMetadataRecord record)
			throws MetaMatrixComponentException {
		if (record.getExtensionProperties() == null) {
			return Collections.emptyList();
		}
		return record.getExtensionProperties();
	}
		
}
