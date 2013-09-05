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
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.connector.DataPlugin;

/**
 * Simple holder for metadata.
 */
public class MetadataStore implements Serializable {
	
	protected static class Grant implements Serializable {
		private static final long serialVersionUID = -3768577122034702953L;
		protected String role;
		protected PermissionMetaData perm;
		
		public Grant(String role, PermissionMetaData perm) {
			this.role = role;
			this.perm = perm;
		}
		
		public String getRole() {
			return role;
		}
		
		public PermissionMetaData getPermission() {
			return perm;
		}
	}

	private static final long serialVersionUID = -3130247626435324312L;
	protected NavigableMap<String, Schema> schemas = new TreeMap<String, Schema>(String.CASE_INSENSITIVE_ORDER);
	protected List<Schema> schemaList = new ArrayList<Schema>(); //used for a stable ordering
	protected NavigableMap<String, Datatype> datatypes = new TreeMap<String, Datatype>(String.CASE_INSENSITIVE_ORDER);
	protected List<Grant> grants;
	private List<String> startTriggers;
	private List<String> shutdownTriggers;
	
	public NavigableMap<String, Schema> getSchemas() {
		return schemas;
	}
	
	public Schema getSchema(String name) {
		return this.schemas.get(name);
	}
	
	public void addSchema(Schema schema) {
		if (this.schemas.put(schema.getName(), schema) != null) {
			throw new DuplicateRecordException(DataPlugin.Event.TEIID60012, DataPlugin.Util.gs(DataPlugin.Event.TEIID60012, schema.getName()));
		}		
		this.schemaList.add(schema);
	}
	
	public List<Schema> getSchemaList() {
		return schemaList;
	}
	
	public void addDataTypes(Collection<Datatype> types) {
		if (types != null){
			for (Datatype type:types) {
				addDatatype(type);
			}
		}
	}
	
	public void addDatatype(Datatype datatype) {
		if (!this.datatypes.containsKey(datatype.getName())) {
			this.datatypes.put(datatype.getName(), datatype);
		}
	}
		
	public NavigableMap<String, Datatype> getDatatypes() {
		return datatypes;
	}
	
	public void merge(MetadataStore store) {
		if (store != null) {
			for (Schema s:store.getSchemaList()) {
				addSchema(s);
			}
			addDataTypes(store.getDatatypes().values());
			addGrants(store.grants);
		}
	}

	void addGrants(List<Grant> g) {
		if (g == null) {
			return;
		}
		if (this.grants == null) {
			this.grants = new ArrayList<Grant>();
		}
		this.grants.addAll(g);
	}

}
