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
package org.teiid.runtime;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.util.StringUtil;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.ContainerLifeCycleListener;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.MaterializationMetadataRepository;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.vdb.runtime.VDBKey;

public abstract class MaterializationManager implements VDBLifeCycleListener {
	
	private interface MaterializationAction {
		void process(Table table);
	}
	
	private Map<VDBKey, List<TimerTask>> scheduledTasks = Collections.synchronizedMap(new HashMap<VDBKey,List<TimerTask>>());
	private ContainerLifeCycleListener shutdownListener;
	
	public MaterializationManager (ContainerLifeCycleListener shutdownListener) {
		this.shutdownListener = shutdownListener;
	}
	
	@Override
	public void added(String name, int version, CompositeVDB cvdb, boolean reloading) {
	}

	@Override
	public void beforeRemove(String name, int version, CompositeVDB cvdb) {
		if (cvdb == null) {
			return;
		}
		final VDBMetaData vdb = cvdb.getVDB();
		
        // cancel any matview load pending tasks
		List<TimerTask> tasks = scheduledTasks.remove(new VDBKey(vdb.getName(), vdb.getVersion()));
		if (tasks != null && !tasks.isEmpty()) {
	        for (TimerTask tt:tasks) {
	        	tt.cancel();
	        }
		}
        
        // If VDB is being undeployed, run the shutdown triggers
		if (!shutdownListener.isShutdownInProgress()) {
			doMaterializationActions(vdb, new MaterializationAction() {
				
				@Override
				public void process(Table table) {
					String remove = table.getProperty(MaterializationMetadataRepository.ON_VDB_DROP_SCRIPT, false);
					if (remove != null) {
						for (String cmd: StringUtil.tokenize(remove, ';')) {
							try {
								executeQuery(vdb, cmd);
							} catch (SQLException e) {
								LogManager.logWarning(LogConstants.CTX_MATVIEWS, e, e.getMessage());
							}
						}					
					}
				}
			});
		}
	}

	@Override
	public void removed(String name, int version, CompositeVDB cvdb) {
	}

	@Override
	public void finishedDeployment(String name, int version, CompositeVDB cvdb, final boolean reloading) {

		// execute start triggers
		final VDBMetaData vdb = cvdb.getVDB();
			doMaterializationActions(vdb, new MaterializationAction() {
				@Override
				public void process(Table table) {
					String start = table.getProperty(MaterializationMetadataRepository.ON_VDB_START_SCRIPT, false);
					if (start != null) {
						for (String script : StringUtil.tokenize(start, ';')) {
							try {
								executeQuery(vdb, script);
							} catch (SQLException e) {
								LogManager.logWarning(LogConstants.CTX_MATVIEWS, e, e.getMessage());
							}
						}
					}
					
					String ttlStr = table.getProperty(MaterializationMetadataRepository.MATVIEW_TTL, false);
					if (ttlStr != null) {
						long ttl = Long.parseLong(ttlStr);
						if (ttl > 0) {
							scheduleJob(vdb, table, ttl, 0L);
						}
					}				
				}
			});
	}
	
	private void doMaterializationActions(VDBMetaData vdb, MaterializationAction action) {
		TransformationMetadata metadata = vdb.getAttachment(TransformationMetadata.class);
		if (metadata == null) {
			return;
		}
		
		Set<String> imports = vdb.getImportedModels();
		MetadataStore store = metadata.getMetadataStore();
		// schedule materialization loads and do the start actions
		for (Schema schema : store.getSchemaList()) {
			if (imports.contains(schema.getName())) {
				continue;
			}
			for (Table table:schema.getTables().values()) {
				// find external matview table
				if (!table.isVirtual() || !table.isMaterialized() 
						|| table.getMaterializedTable() == null
						|| !Boolean.valueOf(table.getProperty(MaterializationMetadataRepository.ALLOW_MATVIEW_MANAGEMENT, false))) {
					continue;
				}
				action.process(table);
			}
		}
	}
	
	public void scheduleJob(VDBMetaData vdb, Table table, long ttl, long delay) {
		TimerTask task = new JobSchedular(vdb, table, ttl, delay);
		queueTask(vdb, task, delay);
	}
	
	private void runJob(VDBMetaData vdb, Table table,  long ttl, long delay) {
		TimerTask task = new QueryJob(vdb, table, ttl, delay);
		queueTask(vdb, task, delay);
	}	
	
	private void queueTask(VDBMetaData vdb, TimerTask task, long delay) {
		VDBKey key = new VDBKey(vdb.getName(), vdb.getVersion());
		List<TimerTask> tasks = scheduledTasks.get(key);
		if (tasks == null) {
			tasks = new ArrayList<TimerTask>();
			scheduledTasks.put(key, tasks);
		}
		synchronized(tasks) {
			tasks.add(task);
		}
		getTimer().schedule(task, (delay < 0)?0:delay);
	}
	
	class JobSchedular extends TimerTask {
		protected Table table;
		protected long ttl;
		protected long delay;
		protected VDBMetaData vdb;
		
		public JobSchedular(VDBMetaData vdb, Table table, long ttl, long delay) {
			this.vdb = vdb;
			this.table = table;
			this.ttl = ttl;
			this.delay = delay;
		}
		
		@Override
		public void run() {
			scheduledTasks.remove(this);
			String query = "execute SYSADMIN.matViewStatus('"+table.getParent().getName()+"', '"+table.getName()+"')"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			
			List<Map<String, String>> result = null;
			try {
				result = executeQuery(vdb, query);
			} catch (SQLException e) {
				LogManager.logWarning(LogConstants.CTX_MATVIEWS, e, e.getMessage());
				scheduleJob(vdb, table, ttl, Math.min(ttl/4, 60000)); // re-schedule the same job in one minute
				return;
			}
			
			long updated = 0L;
			this.delay = ttl;
			String loadstate = null;
			boolean valid = false;
			if (result != null && !result.isEmpty()) {
				Map<String, String> row = result.get(0);
				if (row != null) {
					loadstate = row.get("LoadState"); //$NON-NLS-1$
					updated = Long.parseLong(row.get("Updated")); //$NON-NLS-1$
					valid = Boolean.parseBoolean(row.get("Valid")); //$NON-NLS-1$
				}
			}
			
			long elapsed = System.currentTimeMillis() - updated;
			if (loadstate == null || loadstate.equalsIgnoreCase("needs_loading") || !valid) { //$NON-NLS-1$
				// no entry found run immediately
				runJob(vdb, table, ttl, 0L); 
			}
			else if (loadstate.equalsIgnoreCase("loading")) { //$NON-NLS-1$
				// if the process is already loading do nothing
			}
			else if (loadstate.equalsIgnoreCase("loaded")) { //$NON-NLS-1$
				if (elapsed >= ttl) {
					runJob(vdb, table, ttl, 0L);
				}
				else {
					scheduleJob(vdb, table, ttl, (ttl-elapsed));
				}
			}
			else if (loadstate.equalsIgnoreCase("failed_load")) { //$NON-NLS-1$
				if (elapsed > ttl/4 || elapsed > 60000) { // exceeds 1/4 of cached time or 5 mins
					runJob(vdb, table, ttl, 0L);
				}
				else {
					scheduleJob(vdb, table, ttl, Math.min(((ttl/4)-elapsed), (60000-elapsed)));
				}
			}
		}
	}	
	
	class QueryJob extends JobSchedular {
		
		public QueryJob(VDBMetaData vdb,Table table, long ttl, long delay) {
			super(vdb, table, ttl, delay);
		}

		@Override
		public void run() {
			scheduledTasks.remove(this);
			getExecutor().execute(new Runnable() {
				@Override
				public void run() {
					String query = "execute SYSADMIN.loadMatView('"+table.getParent().getName()+"','"+table.getName()+"')"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
					try {
						executeQuery(vdb, query);
						scheduleJob(vdb, table, ttl, ttl);
					} catch (SQLException e) {
						LogManager.logWarning(LogConstants.CTX_MATVIEWS, e, e.getMessage());
						scheduleJob(vdb, table, ttl, Math.min(ttl/4, 60000)); // re-schedule the same job in one minute
						return;						
					}
				}
			});
		}
	}	
		
	public abstract Timer getTimer();
	public abstract Executor getExecutor();
	public abstract List<Map<String, String>> executeQuery(VDBMetaData vdb, String cmd) throws SQLException;
}
