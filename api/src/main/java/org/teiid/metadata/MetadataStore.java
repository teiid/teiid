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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Simple holder for metadata.
 */
public class MetadataStore implements Serializable {

	private static final long serialVersionUID = -3130247626435324312L;
	protected Map<String, Schema> schemas = new TreeMap<String, Schema>(String.CASE_INSENSITIVE_ORDER);
	protected List<Schema> schemaList = new ArrayList<Schema>(); //used for a stable ordering
	protected Collection<Datatype> datatypes = new LinkedHashSet<Datatype>();
	
	public Map<String, Schema> getSchemas() {
		return schemas;
	}
	
	public void addSchema(Schema schema) {
		this.schemas.put(schema.getName(), schema);
		this.schemaList.add(schema);
	}
	
	public List<Schema> getSchemaList() {
		return schemaList;
	}
	
	public void addDatatype(Datatype datatype) {
		this.datatypes.add(datatype);
	}
		
	/**
	 * Get the datatypes defined in this store
	 * @return
	 */
	public Collection<Datatype> getDatatypes() {
		return datatypes;
	}
	
}
