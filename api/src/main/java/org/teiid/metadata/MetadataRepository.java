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


import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

/**
 * A hook for externalizing view, procedure, and other metadata.
 */
public abstract class MetadataRepository<F,C> {

	/**
	 * Loads the schema information for the vdb for the given schemaName. Loads table, procedures, functions, indexes etc.
	 * @param factory
	 * @param executionFactory may be null if loading a virtual source
	 * @param connectionFactory may be null if source is not available
	 * @return
	 * @throws TranslatorException to indicate a recoverable error, otherwise a RuntimeException
	 */
	public abstract void loadMetadata(MetadataFactory factory, ExecutionFactory<F, C> executionFactory, F connectionFactory) throws TranslatorException;

	/**
	 * Call back function, when "alter view" definition is called 
	 * @param vdbName
	 * @param vdbVersion
	 * @param table
	 * @param viewDefinition
	 */
	public void setViewDefinition(String vdbName, int vdbVersion, Table table, String viewDefinition) {}
	
	/**
	 * Call back function, when "alter trigger" is called 
	 * @param vdbName
	 * @param vdbVersion
	 * @param table
	 * @param triggerOperation
	 * @param triggerDefinition
	 */
	public void setInsteadOfTriggerDefinition(String vdbName, int vdbVersion, Table table, Table.TriggerEvent triggerOperation, String triggerDefinition) {}
	
	/**
	 * Callback function, when "alter trigger" is called to enable or disable a trigger
	 * @param vdbName
	 * @param vdbVersion
	 * @param table
	 * @param triggerOperation
	 * @param enabled
	 */
	public void setInsteadOfTriggerEnabled(String vdbName, int vdbVersion, Table table, Table.TriggerEvent triggerOperation, boolean enabled) {}
	
	
	/**
	 * Call back function, when "alter procedure" is called to set the procedure definition
	 * @param vdbName
	 * @param vdbVersion
	 * @param procedure
	 * @param procedureDefinition
	 */
	public void setProcedureDefinition(String vdbName, int vdbVersion, Procedure procedure, String procedureDefinition) {}
		
	/**
	 * Set the {@link TableStats} for the given table
	 * @param vdbName
	 * @param vdbVersion
	 * @param table
	 * @param tableStats
	 */
	public void setTableStats(String vdbName, int vdbVersion, Table table, TableStats tableStats) {}
	
	
	/**
	 * Set the {@link ColumnStats} for a given column
	 * @param vdbName
	 * @param vdbVersion
	 * @param column
	 * @param columnStats
	 */
	public void setColumnStats(String vdbName, int vdbVersion, Column column, ColumnStats columnStats) {}
	
	/**
	 * Set an extension metadata property for a given record.
	 * @param vdbName
	 * @param vdbVersion
	 * @param record
	 * @param name
	 * @param value
	 */
	public void setProperty(String vdbName, int vdbVersion, AbstractMetadataRecord record, String name, String value) {}
	
}
