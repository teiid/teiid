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

import java.util.LinkedHashMap;
import java.util.Map;

public class Schema extends AbstractMetadataRecord {

    private boolean physical = true;
    private boolean isVisible = true;
    private String primaryMetamodelUri;
    
    private Map<String, Table> tables = new LinkedHashMap<String, Table>();
	private Map<String, ProcedureRecordImpl> procedures = new LinkedHashMap<String, ProcedureRecordImpl>();
	
	public void addTable(Table table) {
		table.setSchema(this);
		this.tables.put(table.getFullName().toLowerCase(), table);
	}
	
	public void addProcedure(ProcedureRecordImpl procedure) {
		procedure.setSchema(this);
		this.procedures.put(procedure.getFullName().toLowerCase(), procedure);
	}

	/**
	 * Get the tables defined in this schema
	 * @return
	 */
	public Map<String, Table> getTables() {
		return tables;
	}
	
	/**
	 * Get the procedures defined in this schema
	 * @return
	 */
	public Map<String, ProcedureRecordImpl> getProcedures() {
		return procedures;
	}
	
    public String getPrimaryMetamodelUri() {
        return primaryMetamodelUri;
    }

    public boolean isVisible() {
        return isVisible;
    }

    public boolean isPhysical() {
        return physical;
    }

    /**
     * @param string
     */
    public void setPrimaryMetamodelUri(String string) {
        primaryMetamodelUri = string;
    }

    /**
     * @param b
     */
    public void setVisible(boolean b) {
        isVisible = b;
    }
    
    public void setPhysical(boolean physical) {
		this.physical = physical;
	}

}
