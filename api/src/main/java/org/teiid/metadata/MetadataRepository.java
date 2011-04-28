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

/**
 * A hook for externalizing view, procedure, and other metadata.
 */
public interface MetadataRepository {
	
	public enum TriggerOperation {
		INSERT,
		UPDATE,
		DELETE
	}
	
	/**
	 * Returns an updated view definition (AS SQL only) or null if the current view definition should be used
	 * should be used.
	 */
	String getViewDefinition(String vdbName, int vdbVersion, Table table);
	
	/**
	 * Set the view definition
	 * @param vdbName
	 * @param vdbVersion
	 * @param table
	 * @param viewDefinition
	 */
	void setViewDefinition(String vdbName, int vdbVersion, Table table, String viewDefinition);

	/**
	 * Returns an updated trigger definition (FOR EACH ROW ...) or null if the current view definition should be used
	 * should be used.
	 */
	String getInsteadOfTriggerDefinition(String vdbName, int vdbVersion, Table table, TriggerOperation triggerOperation);
	
	/**
	 * 
	 * @param vdbName
	 * @param vdbVersion
	 * @param table
	 * @param triggerOperation
	 * @param triggerDefinition
	 */
	void setInsteadOfTriggerDefinition(String vdbName, int vdbVersion, Table table, TriggerOperation triggerOperation, String triggerDefinition);
	
	/**
	 * Returns an updated procedure definition (CREATE PROCEDURE ...) or null if the current procedure definition should be used
	 * should be used.
	 */
	String getProcedureDefinition(String vdbName, int vdbVersion, Procedure procedure);
	
	/**
	 * Set the procedure definition
	 * @param vdbName
	 * @param vdbVersion
	 * @param table
	 * @param procedureDefinition
	 */
	void setProcedureDefinition(String vdbName, int vdbVersion, Procedure table, String procedureDefinition);
	
	/**
	 * Get updated {@link TableStats} for the given table
	 * @param vdbName
	 * @param vdbVersion
	 * @param table
	 * @return the stats.  a null result or a null stat indicates that the current value should be used
	 */
	TableStats getTableStats(String vdbName, int vdbVersion, Table table);
	
	/**
	 * Set the {@link TableStats} for the given table
	 * @param vdbName
	 * @param vdbVersion
	 * @param table
	 * @param tableStats
	 */
	void setTableStats(String vdbName, int vdbVersion, Table table, TableStats tableStats);
	
	/**
	 * Get updated {@link ColumnStats} for the given column
	 * @param vdbName
	 * @param vdbVersion
	 * @param column
	 * @return the stats.  a null result or a null stat indicates that the default should be used
	 */
	ColumnStats getColumnStats(String vdbName, int vdbVersion, Column column);
	
	/**
	 * Set the {@link ColumnStats} for a given column
	 * @param vdbName
	 * @param vdbVersion
	 * @param column
	 * @param columnStats
	 */
	void setColumnStats(String vdbName, int vdbVersion, Column column, ColumnStats columnStats);
}
