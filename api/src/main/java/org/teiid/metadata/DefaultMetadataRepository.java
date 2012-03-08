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

import java.util.LinkedHashMap;

/**
 * This class is being provided for sole reason to inject metadata as it used to be in previous 
 * teiid versions. Take a look at modified interface of the MetadataRepostiory interface.
 */
@SuppressWarnings("unused")
public abstract class DefaultMetadataRepository implements MetadataRepository {

	/**
	 * Marks the start of vdb metadata loading
	 * @param vdbName
	 * @param vdbVersion
	 */
	public void startLoadVdb(String vdbName, int vdbVersion) {
	}
	
	/**
	 * Marks the end of vdb metadata loading
	 * @param vdbName
	 * @param vdbVersion
	 */
	public void endLoadVdb(String vdbName, int vdbVersion) {
	}
	
	/**
	 * Get updated {@link ColumnStats} for the given column
	 * @param vdbName
	 * @param vdbVersion
	 * @param column
	 * @return the stats.  a null result or a null stat indicates that the default should be used
	 */
	public ColumnStats getColumnStats(String vdbName, int vdbVersion, Column column) {
		return null;
	}
	
	/**
	 * Returns an updated trigger definition (FOR EACH ROW ...) or null if the current view definition should be used
	 * should be used.
	 */
	public String getInsteadOfTriggerDefinition(String vdbName, int vdbVersion, Table table, Table.TriggerEvent triggerOperation) {
		return null;
	}
	
	/**
	 * Returns an updated procedure definition (CREATE PROCEDURE ...) or null if the current procedure definition should be used
	 * should be used.
	 */
	public String getProcedureDefinition(String vdbName, int vdbVersion, Procedure procedure) {
		return null;
	}
	
	/**
	 * Get updated {@link TableStats} for the given table
	 * @param vdbName
	 * @param vdbVersion
	 * @param table
	 * @return the stats.  a null result or a null stat indicates that the current value should be used
	 */
	public TableStats getTableStats(String vdbName, int vdbVersion, Table table) {
		return null;
	}
	
	/**
	 * Returns an updated view definition (AS SQL only) or null if the current view definition should be used
	 * should be used.
	 */
	public String getViewDefinition(String vdbName, int vdbVersion, Table table) {
		return null;
	}
	
	/**
	 * Get the extension metadata for a given record.
	 * @param vdbName
	 * @param vdbVersion
	 * @param record
	 * @return
	 */
	public LinkedHashMap<String, String> getProperties(String vdbName, int vdbVersion, AbstractMetadataRecord record){
		return null;
	}
	
	/**
	 * Returns whether the trigger is enabled
	 * @param vdbName
	 * @param vdbVersion
	 * @param table
	 * @param triggerOperation
	 * @return
	 */
	public Boolean isInsteadOfTriggerEnabled(String vdbName, int vdbVersion, Table table, Table.TriggerEvent triggerOperation) {
		return null;
	}
}
