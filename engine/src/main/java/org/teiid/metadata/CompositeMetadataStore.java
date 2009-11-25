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
import java.util.LinkedList;
import java.util.List;

import org.teiid.connector.metadata.runtime.Column;
import org.teiid.connector.metadata.runtime.MetadataStore;
import org.teiid.connector.metadata.runtime.ProcedureRecordImpl;
import org.teiid.connector.metadata.runtime.Schema;
import org.teiid.connector.metadata.runtime.Table;
import org.teiid.connector.metadata.runtime.Table.Type;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.metadata.runtime.api.MetadataSource;
import com.metamatrix.query.QueryPlugin;

/**
 * Aggregates the metadata from multiple stores.  
 * IMPORTANT: All strings queries should be in lower case.
 */
public class CompositeMetadataStore extends MetadataStore {

	private MetadataSource metadataSource;
	
	public CompositeMetadataStore(List<MetadataStore> metadataStores, MetadataSource metadataSource) {
		this.metadataSource = metadataSource;
		for (MetadataStore metadataStore : metadataStores) {
			this.schemas.putAll(metadataStore.getSchemas());
			this.datatypes.addAll(metadataStore.getDatatypes());
		}
	}
	
	public Schema getSchema(String fullName)
			throws QueryMetadataException {
		Schema result = getSchemas().get(fullName);
		if (result == null) {
	        throw new QueryMetadataException(fullName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		return result;
	}
	
	public Table findGroup(String fullName)
			throws QueryMetadataException {
		int index = fullName.indexOf(TransformationMetadata.DELIMITER_STRING);
		if (index == -1) {
		    throw new QueryMetadataException(fullName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}			
		String schema = fullName.substring(0, index);
		Table result = getSchema(schema).getTables().get(fullName.substring(index + 1));
		if (result == null) {
	        throw new QueryMetadataException(fullName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		return result;
	}
	
	/**
	 * TODO: this resolving mode allows partial matches of a full group name containing .
	 * @param partialGroupName
	 * @return
	 */
	public Collection<String> getGroupsForPartialName(String partialGroupName) {
		List<String> result = new LinkedList<String>();
		for (Schema schema : getSchemas().values()) {
			for (Table t : schema.getTables().values()) {
				String fullName = t.getFullName();
				if (fullName.regionMatches(true, fullName.length() - partialGroupName.length(), partialGroupName, 0, partialGroupName.length())) {
					result.add(fullName);	
				}
			}
		}
		return result;
	}
	
	public ProcedureRecordImpl getStoredProcedure(String name)
			throws MetaMatrixComponentException, QueryMetadataException {
		int index = name.indexOf(TransformationMetadata.DELIMITER_STRING);
		if (index > -1) {
			String schema = name.substring(0, index);
			ProcedureRecordImpl result = getSchema(schema).getProcedures().get(name.substring(index + 1));
			if (result != null) {
		        return result;
			}	
		}
		ProcedureRecordImpl result = null;
		//assume it's a partial name
		name = TransformationMetadata.DELIMITER_STRING + name;
		for (Schema schema : getSchemas().values()) {
			for (ProcedureRecordImpl p : schema.getProcedures().values()) {
				String fullName = p.getFullName();
				if (fullName.regionMatches(true, fullName.length() - name.length(), name, 0, name.length())) {
					if (result != null) {
						throw new QueryMetadataException(QueryPlugin.Util.getString("ambiguous_procedure", name.substring(1))); //$NON-NLS-1$
					}
					result = p;	
				}
			}
		}
		if (result == null) {
	        throw new QueryMetadataException(name+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		return result;
	}

	public MetadataSource getMetadataSource() {
		return metadataSource;
	}
	
	/*
	 * The next methods are hold overs from XML/UUID resolving and will perform poorly
	 */
	
	public Column findElement(String fullName) throws QueryMetadataException {
		int columnIndex = fullName.lastIndexOf(TransformationMetadata.DELIMITER_STRING);
		if (columnIndex == -1) {
			throw new QueryMetadataException(fullName+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		Table table = findGroup(fullName.substring(0, columnIndex));
		String shortElementName = fullName.substring(columnIndex + 1);
		for (Column column : table.getColumns()) {
			if (column.getName().equalsIgnoreCase(shortElementName)) {
				return column;
			}
        }
        throw new QueryMetadataException(fullName+TransformationMetadata.NOT_EXISTS_MESSAGE);
	}

	public Collection<Table> getXMLTempGroups(Table tableRecord) {
		ArrayList<Table> results = new ArrayList<Table>();
		String namePrefix = tableRecord.getFullName() + TransformationMetadata.DELIMITER_STRING;
		for (Table table : tableRecord.getSchema().getTables().values()) {
			if (table.getTableType() == Type.XmlStagingTable && table.getName().startsWith(namePrefix)) {
				results.add(table);
			}
		}
		return results;
	}
	
}
