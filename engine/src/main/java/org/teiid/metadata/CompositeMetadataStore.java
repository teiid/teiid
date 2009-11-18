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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.connector.metadata.runtime.Column;
import org.teiid.connector.metadata.runtime.MetadataStore;
import org.teiid.connector.metadata.runtime.Schema;
import org.teiid.connector.metadata.runtime.ProcedureRecordImpl;
import org.teiid.connector.metadata.runtime.Table;
import org.teiid.connector.metadata.runtime.Table.Type;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.core.id.UUID;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.metadata.runtime.api.MetadataSource;

/**
 * Aggregates the metadata from multiple stores.  
 * IMPORTANT: All strings queries should be in lower case.
 */
public class CompositeMetadataStore {

	private MetadataSource metadataSource;
	private Map<String, MetadataStore> storeMap = new LinkedHashMap<String, MetadataStore>();
	private List<MetadataStore> metadataStores;
	
	public CompositeMetadataStore(List<MetadataStore> metadataStores, MetadataSource metadataSource) {
		this.metadataSource = metadataSource;
		this.metadataStores = metadataStores;
		for (MetadataStore metadataStore : metadataStores) {
			for (String model : metadataStore.getSchemas().keySet()) {
				storeMap.put(model.toLowerCase(), metadataStore);
			}
		}
	}
	
	public Schema getModel(String fullName)
			throws QueryMetadataException, MetaMatrixComponentException {
		Schema result = getMetadataStore(fullName).getSchemas().get(fullName);
		if (result == null) {
	        throw new QueryMetadataException(fullName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		return result;
	}
	
	public Table findGroup(String fullName)
			throws QueryMetadataException {
		List<String> tokens = StringUtil.getTokens(fullName, TransformationMetadata.DELIMITER_STRING);
		if (tokens.size() < 2) {
		    throw new QueryMetadataException(fullName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}			
		Table result = getMetadataStore(tokens.get(0)).getSchemas().get(tokens.get(0)).getTables().get(fullName);
		if (result == null) {
	        throw new QueryMetadataException(fullName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		return result;
	}
	
	public Collection<String> getGroupsForPartialName(String partialGroupName) {
		List<String> result = new LinkedList<String>();
		for (MetadataStore store : metadataStores) {
			for (Schema schema : store.getSchemas().values()) {
				for (Map.Entry<String, Table> entry : schema.getTables().entrySet()) {
					if (entry.getKey().endsWith(partialGroupName)) {
						result.add(entry.getValue().getFullName());
					}
				}
			}
		}
		return result;
	}
	
	public ProcedureRecordImpl getStoredProcedure(
			String fullyQualifiedProcedureName)
			throws MetaMatrixComponentException, QueryMetadataException {
		List<String> tokens = StringUtil.getTokens(fullyQualifiedProcedureName, TransformationMetadata.DELIMITER_STRING);
		if (tokens.size() < 2) {
		    throw new QueryMetadataException(fullyQualifiedProcedureName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		ProcedureRecordImpl result = getMetadataStore(tokens.get(0)).getSchemas().get(tokens.get(0)).getProcedures().get(fullyQualifiedProcedureName);
		if (result == null) {
	        throw new QueryMetadataException(fullyQualifiedProcedureName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		return result;
	}

	private MetadataStore getMetadataStore(String modelName) throws QueryMetadataException {
		MetadataStore store = this.storeMap.get(modelName);
		if (store == null) {
			throw new QueryMetadataException(modelName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		return store;
	}

	public MetadataSource getMetadataSource() {
		return metadataSource;
	}
	
	public List<MetadataStore> getMetadataStores() {
		return metadataStores;
	}

	/*
	 * The next methods are hold overs from XML/UUID resolving and will perform poorly
	 */
	
	public Column findElement(String elementName) throws QueryMetadataException {
		if (StringUtil.startsWithIgnoreCase(elementName,UUID.PROTOCOL)) {
			for (MetadataStore store : this.metadataStores) {
				for (Schema schema : store.getSchemas().values()) {
					for (Table table : schema.getTables().values()) {
						for (Column column : table.getColumns()) {
							if (column.getUUID().equalsIgnoreCase(elementName)) {
								return column;
							}
						}
					}
				}
			}
        } else {
        	List<String> tokens = StringUtil.getTokens(elementName, TransformationMetadata.DELIMITER_STRING);
    		if (tokens.size() < 3) {
    		    throw new QueryMetadataException(elementName+TransformationMetadata.NOT_EXISTS_MESSAGE);
    		}
    		Table table = findGroup(StringUtil.join(tokens.subList(0, tokens.size() - 1), TransformationMetadata.DELIMITER_STRING));
    		for (Column column : table.getColumns()) {
    			if (column.getFullName().equalsIgnoreCase(elementName)) {
    				return column;
    			}
			}
        }
        throw new QueryMetadataException(elementName+TransformationMetadata.NOT_EXISTS_MESSAGE);
	}

	public Collection<Table> getXMLTempGroups(Table tableRecord) throws QueryMetadataException {
		ArrayList<Table> results = new ArrayList<Table>();
		MetadataStore store = getMetadataStore(tableRecord.getSchema().getName().toLowerCase());
		String namePrefix = tableRecord.getFullName() + TransformationMetadata.DELIMITER_STRING;
		for (Schema schema : store.getSchemas().values()) {
			for (Table table : schema.getTables().values()) {
				if (table.getTableType() == Type.XmlStagingTable && table.getName().startsWith(namePrefix)) {
					results.add(table);
				}
			}
		}
		return results;
	}
	
}
