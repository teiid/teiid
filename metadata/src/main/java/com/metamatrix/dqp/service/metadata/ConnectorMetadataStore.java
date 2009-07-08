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

package com.metamatrix.dqp.service.metadata;

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
import org.teiid.connector.metadata.runtime.PropertyRecordImpl;
import org.teiid.connector.metadata.runtime.TableRecordImpl;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.modeler.internal.core.index.IndexConstants;
import com.metamatrix.query.metadata.MetadataStore;
import com.metamatrix.query.metadata.StoredProcedureInfo;
import com.metamatrix.vdb.runtime.VDBKey;

public class ConnectorMetadataStore implements MetadataStore {
	
	private ConnectorMetadata metadata;
	private String modelName;
	
	public ConnectorMetadataStore(VDBKey key, String modelName, DataService dataService) throws MetaMatrixComponentException {
		this.modelName = modelName;
		this.metadata = dataService.getConnectorMetadata(key.getName(), key.getVersion(), modelName);
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
	public StoredProcedureInfo getStoredProcedureInfoForProcedure(
			String fullyQualifiedProcedureName)
			throws MetaMatrixComponentException, QueryMetadataException {
		// TODO Auto-generated method stub
		return null;
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
		case IndexConstants.RECORD_TYPE.CALLABLE:
			return metadata.getProcedures();
		case IndexConstants.RECORD_TYPE.CALLABLE_PARAMETER:
			return Collections.emptyList();
		case IndexConstants.RECORD_TYPE.RESULT_SET:
			return Collections.emptyList();
			
		case IndexConstants.RECORD_TYPE.ACCESS_PATTERN: {
			Collection<ColumnSetRecordImpl> results = new ArrayList<ColumnSetRecordImpl>();
			for (TableRecordImpl table : metadata.getTables()) {
				if (table.getAccessPatterns() != null) {
					results.addAll(table.getAccessPatterns());
				}
			}
			return results;
		}
		case IndexConstants.RECORD_TYPE.UNIQUE_KEY: {
			Collection<ColumnSetRecordImpl> results = new ArrayList<ColumnSetRecordImpl>();
			for (TableRecordImpl table : metadata.getTables()) {
				if (table.getUniqueKeys() != null) {
					results.addAll(table.getUniqueKeys());
				}
			}
			return results;
		}
		case IndexConstants.RECORD_TYPE.PRIMARY_KEY: {
			Collection<ColumnSetRecordImpl> results = new ArrayList<ColumnSetRecordImpl>();
			for (TableRecordImpl table : metadata.getTables()) {
				if (table.getPrimaryKey() != null) {
					results.add(table.getPrimaryKey());
				}
			}
			return results;
		}
		case IndexConstants.RECORD_TYPE.FOREIGN_KEY: {
			Collection<ColumnSetRecordImpl> results = new ArrayList<ColumnSetRecordImpl>();
			for (TableRecordImpl table : metadata.getTables()) {
				if (table.getForeignKeys() != null) {
					results.addAll(table.getForeignKeys());
				}
			}
			return results;
		}
		case IndexConstants.RECORD_TYPE.INDEX: {
			Collection<ColumnSetRecordImpl> results = new ArrayList<ColumnSetRecordImpl>();
			for (TableRecordImpl table : metadata.getTables()) {
				if (table.getIndexes() != null) {
					results.addAll(table.getIndexes());
				}
			}
			return results;
		}
		case IndexConstants.RECORD_TYPE.MODEL:
			return Arrays.asList(metadata.getModel());
		case IndexConstants.RECORD_TYPE.TABLE:
			return metadata.getTables();
		case IndexConstants.RECORD_TYPE.COLUMN: {
			Collection<ColumnRecordImpl> results = new ArrayList<ColumnRecordImpl>();
			for (TableRecordImpl table : metadata.getTables()) {
				if (table.getColumns() != null) {
					results.addAll(table.getColumns());
				}
			}
			return results;
		}
		}
		return Collections.emptyList();
	}

	@Override
	public Collection<? extends AbstractMetadataRecord> findMetadataRecords(String indexName,
			String pattern, boolean isPrefix,
			boolean isCaseSensitive) throws MetaMatrixCoreException {
		if (indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.COLUMNS_INDEX)) {
			return getRecordsByType(IndexConstants.RECORD_TYPE.COLUMN);
		} else if (indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.KEYS_INDEX)) {
			List<AbstractMetadataRecord> result = new ArrayList<AbstractMetadataRecord>();
			result.addAll(getRecordsByType(IndexConstants.RECORD_TYPE.ACCESS_PATTERN));
			result.addAll(getRecordsByType(IndexConstants.RECORD_TYPE.UNIQUE_KEY));
			result.addAll(getRecordsByType(IndexConstants.RECORD_TYPE.PRIMARY_KEY));
			result.addAll(getRecordsByType(IndexConstants.RECORD_TYPE.FOREIGN_KEY));
			result.addAll(getRecordsByType(IndexConstants.RECORD_TYPE.INDEX));
			return result;
		} else if (indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.MODELS_INDEX)) {
			return getRecordsByType(IndexConstants.RECORD_TYPE.MODEL);
		} else if (indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.PROCEDURES_INDEX)) {
			List<AbstractMetadataRecord> result = new ArrayList<AbstractMetadataRecord>();
			result.addAll(getRecordsByType(IndexConstants.RECORD_TYPE.CALLABLE));
			result.addAll(getRecordsByType(IndexConstants.RECORD_TYPE.CALLABLE_PARAMETER));
			result.addAll(getRecordsByType(IndexConstants.RECORD_TYPE.RESULT_SET));
			return result;
		} else if (indexName.equalsIgnoreCase(IndexConstants.INDEX_NAME.TABLES_INDEX)) {
			return getRecordsByType(IndexConstants.RECORD_TYPE.TABLE);
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
