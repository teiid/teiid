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

package org.teiid.query.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.Type;


/**
 * Aggregates the metadata from multiple stores.  
 * IMPORTANT: All strings queries should be in upper case.
 */
public class CompositeMetadataStore extends MetadataStore {

	
	public CompositeMetadataStore(MetadataStore metadataStore) {
		addMetadataStore(metadataStore);
	}
	
	public CompositeMetadataStore(List<MetadataStore> metadataStores) {
		for (MetadataStore metadataStore : metadataStores) {
			addMetadataStore(metadataStore);
		}
	}
	
	public void addMetadataStore(MetadataStore metadataStore) {
		this.schemas.putAll(metadataStore.getSchemas());
		this.datatypes.addAll(metadataStore.getDatatypes());
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
	public Collection<Table> getGroupsForPartialName(String partialGroupName) {
		List<Table> result = new LinkedList<Table>();
		for (Schema schema : getSchemas().values()) {
			for (Table t : schema.getTables().values()) {
				String fullName = t.getFullName();
				if (fullName.regionMatches(true, fullName.length() - partialGroupName.length(), partialGroupName, 0, partialGroupName.length())) {
					result.add(t);	
				}
			}
		}
		return result;
	}
	
	public Collection<Procedure> getStoredProcedure(String name)
			throws TeiidComponentException, QueryMetadataException {
		List<Procedure> result = new LinkedList<Procedure>();
		int index = name.indexOf(TransformationMetadata.DELIMITER_STRING);
		if (index > -1) {
			String schema = name.substring(0, index);
			Procedure proc = getSchema(schema).getProcedures().get(name.substring(index + 1));
			if (proc != null) {
				result.add(proc);
		        return result;
			}	
		}
		//assume it's a partial name
		name = TransformationMetadata.DELIMITER_STRING + name;
		for (Schema schema : getSchemas().values()) {
			for (Procedure p : schema.getProcedures().values()) {
				String fullName = p.getFullName();
				if (fullName.regionMatches(true, fullName.length() - name.length(), name, 0, name.length())) {
					result.add(p);	
				}
			}
		}
		if (result.isEmpty()) {
	        throw new QueryMetadataException(name.substring(1)+TransformationMetadata.NOT_EXISTS_MESSAGE);
		}
		return result;
	}

	/*
	 * The next method is a hold over from XML/UUID resolving and will perform poorly
	 */

	public Collection<Table> getXMLTempGroups(Table tableRecord) {
		ArrayList<Table> results = new ArrayList<Table>();
		String namePrefix = tableRecord.getFullName() + TransformationMetadata.DELIMITER_STRING;
		for (Table table : tableRecord.getParent().getTables().values()) {
			if (table.getTableType() == Type.XmlStagingTable && table.getName().startsWith(namePrefix)) {
				results.add(table);
			}
		}
		return results;
	}
	
}
