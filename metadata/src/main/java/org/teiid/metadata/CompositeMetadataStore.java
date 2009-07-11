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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.ColumnRecordImpl;
import org.teiid.connector.metadata.runtime.ModelRecordImpl;
import org.teiid.connector.metadata.runtime.ProcedureRecordImpl;
import org.teiid.connector.metadata.runtime.PropertyRecordImpl;
import org.teiid.connector.metadata.runtime.TableRecordImpl;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.metadata.runtime.api.MetadataSource;
import com.metamatrix.query.metadata.MetadataStore;

public class CompositeMetadataStore implements MetadataStore {

	private List<? extends MetadataStore> metadataStores;
	private MetadataSource metadataSource;
	private Map<String, MetadataStore> storeMap;
	
	public CompositeMetadataStore(List<? extends MetadataStore> metadataStores, MetadataSource metadataSource) {
		this.metadataStores = metadataStores;
		this.metadataSource = metadataSource;
		this.storeMap = new HashMap<String, MetadataStore>();
		for (MetadataStore metadataStore : metadataStores) {
			for (String model : metadataStore.getModelNames()) {
				storeMap.put(model.toUpperCase(), metadataStore);
			}
		}
	}
	
	@Override
	public Collection<String> getModelNames() {
		return storeMap.keySet();
	}
	
	@Override
	public ModelRecordImpl getModel(String fullName)
			throws QueryMetadataException, MetaMatrixComponentException {
		return getMetadataStore(fullName).getModel(fullName);
	}
	
	@Override
	public ColumnRecordImpl findElement(String fullName)
			throws QueryMetadataException, MetaMatrixComponentException {
		List<String> tokens = StringUtil.getTokens(fullName, TransformationMetadata.DELIMITER_STRING);
		if (tokens.size() < 3) {
		    throw new QueryMetadataException(fullName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}			
		return getMetadataStore(tokens.get(0)).findElement(fullName);
	}
	
	@Override
	public TableRecordImpl findGroup(String fullName)
			throws QueryMetadataException, MetaMatrixComponentException {
		List<String> tokens = StringUtil.getTokens(fullName, TransformationMetadata.DELIMITER_STRING);
		if (tokens.size() < 2) {
		    throw new QueryMetadataException(fullName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}			
		return getMetadataStore(tokens.get(0)).findGroup(fullName);
	}
	
	@Override
	public Collection<String> getGroupsForPartialName(String partialGroupName)
			throws MetaMatrixComponentException, QueryMetadataException {
		List<String> result = new LinkedList<String>();
		for (MetadataStore store : metadataStores) {
			result.addAll(store.getGroupsForPartialName(partialGroupName));
		}
		return result;
	}
	
	@Override
	public Collection getXMLTempGroups(TableRecordImpl table)
			throws MetaMatrixComponentException {
		return getMetadataStore(table.getModelName()).getXMLTempGroups(table);
	}
	
	@Override
	public ProcedureRecordImpl getStoredProcedure(
			String fullyQualifiedProcedureName)
			throws MetaMatrixComponentException, QueryMetadataException {
		List<String> tokens = StringUtil.getTokens(fullyQualifiedProcedureName, TransformationMetadata.DELIMITER_STRING);
		if (tokens.size() < 2) {
		    throw new QueryMetadataException(fullyQualifiedProcedureName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		return getMetadataStore(tokens.get(0)).getStoredProcedure(fullyQualifiedProcedureName);
	}

	@Override
	public Collection<? extends AbstractMetadataRecord> findMetadataRecords(char recordType,
			String entityName, boolean isPartialName)
			throws MetaMatrixComponentException {
		LinkedList<AbstractMetadataRecord> result = new LinkedList<AbstractMetadataRecord>();
		for (MetadataStore store : metadataStores) {
			Collection<? extends AbstractMetadataRecord> results = store.findMetadataRecords(recordType, entityName, isPartialName);
			if (!results.isEmpty() && !isPartialName) {
				return results;
			}
			result.addAll(results);
		}
		return result;
	}
	
	public boolean postProcessFindMetadataRecords() {
		for (MetadataStore store : metadataStores) {
			if (store.postProcessFindMetadataRecords()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Collection<AbstractMetadataRecord> findMetadataRecords(String indexName,
			String pattern, boolean isPrefix,
			boolean isCaseSensitive) throws MetaMatrixCoreException {
		LinkedList<AbstractMetadataRecord> result = new LinkedList<AbstractMetadataRecord>();
		for (MetadataStore store : metadataStores) {
			Collection<? extends AbstractMetadataRecord> results = store.findMetadataRecords(indexName, pattern, isPrefix, isCaseSensitive);
			result.addAll(results);
		}
		return result;
	}

	@Override
	public Collection<PropertyRecordImpl> getExtensionProperties(AbstractMetadataRecord record)
			throws MetaMatrixComponentException {
		return getMetadataStore(record.getModelName()).getExtensionProperties(record);
	}

	public MetadataStore getMetadataStore(String modelName) throws QueryMetadataException {
		MetadataStore store = this.storeMap.get(modelName.toUpperCase());
		if (store == null) {
			throw new QueryMetadataException(modelName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		return store;
	}

	public MetadataSource getMetadataSource() {
		return metadataSource;
	}
	
}
