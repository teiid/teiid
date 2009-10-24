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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple holder for metadata.
 */
public class MetadataStore implements Serializable {

	private static final long serialVersionUID = -5276588417872522795L;
	private Map<String, ModelRecordImpl> models = new LinkedHashMap<String, ModelRecordImpl>();
	private Map<String, TableRecordImpl> tables = new LinkedHashMap<String, TableRecordImpl>();
	private Map<String, ProcedureRecordImpl> procedures = new LinkedHashMap<String, ProcedureRecordImpl>();
	private Collection<DatatypeRecordImpl> datatypes = new ArrayList<DatatypeRecordImpl>();
	
	public void addModel(ModelRecordImpl model) {
		this.models.put(model.getFullName().toLowerCase(), model);
	}
	
	public void addTable(TableRecordImpl table) {
		this.tables.put(table.getFullName().toLowerCase(), table);
	}
	
	public void addProcedure(ProcedureRecordImpl procedure) {
		this.procedures.put(procedure.getFullName().toLowerCase(), procedure);
	}
	
	public void addDatatype(DatatypeRecordImpl datatype) {
		this.datatypes.add(datatype);
	}
		
	/**
	 * Get the models represented in this store.  
	 * For a connector there will only be 1 model and the model name is treated as 
	 * a top level schema for all source metadata.
	 * @return
	 */
	public Map<String, ModelRecordImpl> getModels() {
		return models;
	}
	
	/**
	 * Get the tables defined in this store
	 * @return
	 */
	public Map<String, TableRecordImpl> getTables() {
		return tables;
	}
	
	/**
	 * Get the procedures defined in this store
	 * @return
	 */
	public Map<String, ProcedureRecordImpl> getProcedures() {
		return procedures;
	}
	
	/**
	 * Get the datatypes defined in this store
	 * @return
	 */
	public Collection<DatatypeRecordImpl> getDatatypes() {
		return datatypes;
	}
	
}
