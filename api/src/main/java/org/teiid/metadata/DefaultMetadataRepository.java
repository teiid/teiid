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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

/**
 * This class is being provided for sole reason to inject metadata as it used to be in previous 
 * teiid versions. Take a look at modified interface of the MetadataRepostiory interface.
 * 
 * If a {@link DefaultMetadataRepository} is used, it will inject metadata onto whatever has been
 * loaded at that point in the repository chain.  Generally this means that a {@link DefaultMetadataRepository}
 * should be last.
 */
@SuppressWarnings("unused")
@Deprecated
public abstract class DefaultMetadataRepository<F, C> extends MetadataRepository<F, C> {

	/**
	 * Calls the specific getter methods of this class to populate metadata on schema being loaded.
	 * If this method is overriden, the super method must be called to perform the metadata injection.
	 */
	public void loadMetadata(MetadataFactory factory, ExecutionFactory<F,C> executionFactory, F connectionFactory) throws TranslatorException {
		String vdbName = factory.getVdbName();
		int vdbVersion = factory.getVdbVersion();
		Collection<AbstractMetadataRecord> records = new LinkedHashSet<AbstractMetadataRecord>();
						
		this.startLoadVdb(vdbName, vdbVersion);
		Schema schema = factory.getSchema();
		records.add(schema);
		for (Table t : schema.getTables().values()) {
			records.add(t);
			records.addAll(t.getColumns());
			records.addAll(t.getAllKeys());
			if (t.isPhysical()) {
				TableStats stats = this.getTableStats(vdbName, vdbVersion, t);
				if (stats != null) {
					t.setTableStats(stats);
				}
				for (Column c : t.getColumns()) {
					ColumnStats cStats = this.getColumnStats(vdbName, vdbVersion, c);
					if (cStats != null) {
						c.setColumnStats(cStats);
					}
				}
			} else {
				String def = this.getViewDefinition(vdbName, vdbVersion, t);
				if (def != null) {
					t.setSelectTransformation(def);
				}
				if (t.supportsUpdate()) {
					def = this.getInsteadOfTriggerDefinition(vdbName, vdbVersion, t, Table.TriggerEvent.INSERT);
					if (def != null) {
						t.setInsertPlan(def);
					}
					Boolean enabled = this.isInsteadOfTriggerEnabled(vdbName, vdbVersion, t, Table.TriggerEvent.INSERT);
					if (enabled != null) {
						t.setInsertPlanEnabled(enabled);
					}
					def = this.getInsteadOfTriggerDefinition(vdbName, vdbVersion, t, Table.TriggerEvent.UPDATE);
					if (def != null) {
						t.setUpdatePlan(def);
					}
					enabled = this.isInsteadOfTriggerEnabled(vdbName, vdbVersion, t, Table.TriggerEvent.UPDATE);
					if (enabled != null) {
						t.setUpdatePlanEnabled(enabled);
					}
					def = this.getInsteadOfTriggerDefinition(vdbName, vdbVersion, t, Table.TriggerEvent.DELETE);
					if (def != null) {
						t.setDeletePlan(def);
					}
					enabled = this.isInsteadOfTriggerEnabled(vdbName, vdbVersion, t, Table.TriggerEvent.DELETE);
					if (enabled != null) {
						t.setDeletePlanEnabled(enabled);
					}
				}
			}
		}
		for (Procedure p : schema.getProcedures().values()) {
			records.add(p);
			records.addAll(p.getParameters());
			if (p.getResultSet() != null) {
				records.addAll(p.getResultSet().getColumns());
			}
			if (p.isVirtual() && !p.isFunction()) {
				String proc = this.getProcedureDefinition(vdbName, vdbVersion, p);
				if (proc != null) {
					p.setQueryPlan(proc);								
				}
			}
		}
	
		for (AbstractMetadataRecord abstractMetadataRecord : records) {
			LinkedHashMap<String, String> p = this.getProperties(vdbName, vdbVersion, abstractMetadataRecord);
			if (p != null) {
				abstractMetadataRecord.setProperties(p);
			}
		}
		this.endLoadVdb(vdbName, vdbVersion);
	}
	
	/**
	 * Marks the start of vdb metadata loading
	 * Note: this is called for every schema
	 * @param vdbName
	 * @param vdbVersion
	 */
	public void startLoadVdb(String vdbName, int vdbVersion) {
	}
	
	/**
	 * Marks the end of vdb metadata loading
	 * Note: this is called for every schema
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
