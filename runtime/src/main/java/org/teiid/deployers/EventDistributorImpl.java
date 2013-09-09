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
package org.teiid.deployers;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidComponentException;
import org.teiid.dqp.internal.process.DataTierManagerImpl;
import org.teiid.events.EventDistributor;
import org.teiid.events.EventListener;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.metadata.TableStats;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.processor.DdlPlan;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.runtime.RuntimePlugin;

public abstract class EventDistributorImpl implements EventDistributor {
	private Set<EventListener> listeners = Collections.newSetFromMap(new ConcurrentHashMap<EventListener, Boolean>());

	public abstract VDBRepository getVdbRepository();
	
	public EventDistributorImpl() {
		getVdbRepository().addListener(new VDBLifeCycleListener() {
			@Override
			public void removed(String name, int version, CompositeVDB vdb) {
				for(EventListener el:EventDistributorImpl.this.listeners) {
					el.vdbUndeployed(name, version);
				}
			}
			@Override
			public void finishedDeployment(String name, int version, CompositeVDB vdb, boolean reloading) {
				for(EventListener el:EventDistributorImpl.this.listeners) {
					if (vdb.getVDB().getStatus().equals(Status.ACTIVE)) {
						el.vdbLoaded(vdb.getVDB());
					}
					else {
						el.vdbLoadFailed(vdb.getVDB());
					}
				}
			}
			@Override
			public void added(String name, int version, CompositeVDB vdb, boolean reloading) {
				for(EventListener el:EventDistributorImpl.this.listeners) {
					el.vdbDeployed(name, version);
				}
			}
			@Override
			public void beforeRemove(String name, int version, CompositeVDB vdb) {
			}			
		});
	}
	
	@Override
	public void updateMatViewRow(String vdbName, int vdbVersion, String schema,
			String viewName, List<?> tuple, boolean delete) {
		VDBMetaData metadata = getVdbRepository().getLiveVDB(vdbName, vdbVersion);
		if (metadata != null) {
			GlobalTableStore gts = metadata.getAttachment(GlobalTableStore.class);
			if (gts != null) {
				try {
					gts.updateMatViewRow((RelationalPlanner.MAT_PREFIX + schema + '.' + viewName).toUpperCase(), tuple, delete);
				} catch (TeiidComponentException e) {
					LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40013, "updateMatViewRow")); //$NON-NLS-1$
				}
			}
		}
	}
	
	@Override
	public void dataModification(String vdbName, int vdbVersion, String schema,	String... tableNames) {
		updateModified(true, vdbName, vdbVersion, schema, tableNames);
	}
	
	private void updateModified(boolean data, String vdbName, int vdbVersion, String schema,
			String... objectNames) {
		Schema s = getSchema(vdbName, vdbVersion, schema);
		if (s == null) {
			return;
		}
		long ts = System.currentTimeMillis();
		for (String name:objectNames) {
			Table table = s.getTables().get(name);
			if (table == null) {
				continue;
			}
			if (data) {
				table.setLastDataModification(ts);
			} else {
				table.setLastModified(ts);
			}
		}
	}
	
	@Override
	public void setColumnStats(String vdbName, int vdbVersion,
			String schemaName, String tableName, String columnName,
			ColumnStats stats) {
		Table t = getTable(vdbName, vdbVersion, schemaName, tableName);
		if (t == null) {
			return;
		}
		Column c = t.getColumnByName(columnName);
		if (c != null) {
			c.setColumnStats(stats);
			t.setLastModified(System.currentTimeMillis());
		}
	}
	
	@Override
	public void setTableStats(String vdbName, int vdbVersion,
			String schemaName, String tableName, TableStats stats) {
		Table t = getTable(vdbName, vdbVersion, schemaName, tableName);
		if (t == null) {
			return;
		}
		t.setTableStats(stats);
		t.setLastModified(System.currentTimeMillis());
	}

	private Table getTable(String vdbName, int vdbVersion, String schemaName,
			String tableName) {
		Schema s = getSchema(vdbName, vdbVersion, schemaName);
		if (s == null) {
			return null;
		}
		return s.getTables().get(tableName);
	}

	private Schema getSchema(String vdbName, int vdbVersion, String schemaName) {
		VDBMetaData vdb = getVdbRepository().getLiveVDB(vdbName, vdbVersion);
		if (vdb == null) {
			return null;
		}
		TransformationMetadata tm = vdb.getAttachment(TransformationMetadata.class);
		if (tm == null) {
			return null;
		}
		return tm.getMetadataStore().getSchemas().get(schemaName);
	}
	
	@Override
	public void setInsteadOfTriggerDefinition(String vdbName, int vdbVersion,
			String schema, String viewName, TriggerEvent triggerEvent,
			String triggerDefinition, Boolean enabled) {
		Table t = getTable(vdbName, vdbVersion, schema, viewName);
		if (t == null) {
			return;
		}
		DdlPlan.alterInsteadOfTrigger(getVdbRepository().getLiveVDB(vdbName, vdbVersion), t, triggerDefinition, enabled, triggerEvent);
	}
	
	@Override
	public void setProcedureDefinition(String vdbName, int vdbVersion,String schema, String procName, String definition) {
		Schema s = getSchema(vdbName, vdbVersion, schema);
		if (s == null) {
			return;
		}
		Procedure p = s.getProcedures().get(procName);
		if (p == null) {
			return;
		}
		DdlPlan.alterProcedureDefinition(getVdbRepository().getLiveVDB(vdbName, vdbVersion), p, definition);
	}
	
	@Override
	public void setViewDefinition(String vdbName, int vdbVersion, String schema, String viewName, String definition) {
		Table t = getTable(vdbName, vdbVersion, schema, viewName);
		if (t == null) {
			return;
		}
		DdlPlan.alterView(getVdbRepository().getLiveVDB(vdbName, vdbVersion), t, definition);
	}
	
	@Override
	public void setProperty(String vdbName, int vdbVersion, String uuid,
			String name, String value) {
		VDBMetaData vdb = getVdbRepository().getLiveVDB(vdbName, vdbVersion);
		if (vdb == null) {
			return;
		}
		TransformationMetadata tm = vdb.getAttachment(TransformationMetadata.class);
		if (tm == null) {
			return;
		}
		AbstractMetadataRecord record = DataTierManagerImpl.getByUuid(tm.getMetadataStore(), uuid);
		if (record != null) {
			record.setProperty(name, value);
		}
	}
	
	@Override
	public void register(EventListener listener) {
		this.listeners.add(listener);
	}
	
	@Override
	public void unregister(EventListener listener) {
		this.listeners.remove(listener);
	}
}
