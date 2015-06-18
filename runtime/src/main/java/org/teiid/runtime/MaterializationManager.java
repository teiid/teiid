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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.util.ResultsFuture;
import org.teiid.client.util.ResultsFuture.CompletionListener;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.StringUtil;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.ContainerLifeCycleListener;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.MaterializationMetadataRepository;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.tempdata.TempTableDataManager;

public abstract class MaterializationManager implements VDBLifeCycleListener {
	
	private interface MaterializationAction {
		void process(Table table);
	}
	
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
		Collection<Future<?>> tasks = cvdb.clearTasks();
		if (tasks != null && !tasks.isEmpty()) {
	        for (Future<?> f:tasks) {
	        	f.cancel(true);
        	}
		}
        
        // If VDB is being undeployed, run the shutdown triggers
		if (!shutdownListener.isShutdownInProgress()) {
			doMaterializationActions(vdb, new MaterializationAction() {
				
				@Override
				public void process(Table table) {
					if (table.getMaterializedTable() == null) {
						return;
					}
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
	public void finishedDeployment(String name, int version, final CompositeVDB cvdb, final boolean reloading) {

		// execute start triggers
		final VDBMetaData vdb = cvdb.getVDB();
			doMaterializationActions(vdb, new MaterializationAction() {
				@Override
				public void process(final Table table) {
					if (table.getMaterializedTable() == null) {
						String ttlStr = table.getProperty(MaterializationMetadataRepository.MATVIEW_TTL, false);
						if (ttlStr != null) {
							long ttl = Long.parseLong(ttlStr);
							if (ttl > 0) {
								//TODO: make the interval based upon the state as with the external, but
								//for now just refresh on schedule
								Future<?> f = getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
									@Override
									public void run() {
										boolean invalidate = TempTableDataManager.shouldInvalidate(vdb);
										try {
											executeAsynchQuery(vdb, "SYSADMIN.refreshMatView(" + table.getSQLString() + ", " + invalidate + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
										} catch (SQLException e) {
											LogManager.logWarning(LogConstants.CTX_MATVIEWS, e, e.getMessage());
										}
									}
								}, 0, ttl, TimeUnit.MILLISECONDS);
								cvdb.addTask(f);
								return;
							}
						}
						//just a one time load
						try {
							//we use a count so that the load can cascade
							executeAsynchQuery(vdb, "select count(*) from " + table.getSQLString()); //$NON-NLS-1$
						} catch (SQLException e) {
							LogManager.logWarning(LogConstants.CTX_MATVIEWS, e, e.getMessage());
						} 
						return;
					}
					if (!reloading) {
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
					}
					
					String ttlStr = table.getProperty(MaterializationMetadataRepository.MATVIEW_TTL, false);
					if (ttlStr == null) {
					    ttlStr = String.valueOf(Long.MAX_VALUE);
					}
					if (ttlStr != null) {
						long ttl = Long.parseLong(ttlStr);
						if (ttl > 0) {
							scheduleJob(cvdb, table, ttl, 0L);
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
						|| !Boolean.valueOf(table.getProperty(MaterializationMetadataRepository.ALLOW_MATVIEW_MANAGEMENT, false))) {
					continue;
				}
				action.process(table);
			}
		}
	}
	
	public void scheduleJob(CompositeVDB vdb, Table table, long ttl, long delay) {
		JobScheduler task = new JobScheduler(vdb, table, ttl, delay);
		queueTask(vdb, task, delay);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void runJob(final CompositeVDB vdb, final Table table, final long ttl) {
		String command = "execute SYSADMIN.loadMatView('"+StringUtil.replaceAll(table.getParent().getName(), "'", "''")+"','"+StringUtil.replaceAll(table.getName(), "'", "''")+"')"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
		try {
			executeAsynchQuery(vdb.getVDB(), command).addCompletionListener(new CompletionListener() {
				@Override
				public void onCompletion(ResultsFuture future) {
					try {
						future.get();
						scheduleJob(vdb, table, ttl, ttl);
					} catch (InterruptedException e) {
					} catch (ExecutionException e) {
						LogManager.logWarning(LogConstants.CTX_MATVIEWS, e, e.getMessage());
						scheduleJob(vdb, table, ttl, Math.min(ttl/4, 60000)); // re-schedule the same job in one minute
					}
				}
			});
		} catch (SQLException e) {
			LogManager.logWarning(LogConstants.CTX_MATVIEWS, e, e.getMessage());
			scheduleJob(vdb, table, ttl, Math.min(ttl/4, 60000)); // re-schedule the same job in one minute
		}
	}	
	
	private void queueTask(CompositeVDB vdb, JobScheduler task, long delay) {
		ScheduledFuture<?> sf = getScheduledExecutorService().schedule(task, (delay < 0)?0:delay, TimeUnit.MILLISECONDS);
		vdb.addTask(sf);
		task.future = sf;
	}
	
	class JobScheduler implements Runnable {
		protected Table table;
		protected long ttl;
		protected long delay;
		protected CompositeVDB vdb;
		protected ScheduledFuture<?> future;
		
		public JobScheduler(CompositeVDB vdb, Table table, long ttl, long delay) {
			this.vdb = vdb;
			this.table = table;
			this.ttl = ttl;
			this.delay = delay;
		}
		
		@Override
		public void run() {
			vdb.removeTask(future);
			String query = "execute SYSADMIN.matViewStatus('"+StringUtil.replaceAll(table.getParent().getName(), "'", "''")+"', '"+StringUtil.replaceAll(table.getName(), "'", "''")+"')"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ 
			
			List<Map<String, String>> result = null;
			try {
				result = executeQuery(vdb.getVDB(), query);
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
			long next = ttl;
			if (loadstate == null || loadstate.equalsIgnoreCase("needs_loading") || !valid) { //$NON-NLS-1$
				// no entry found run immediately
				runJob(vdb, table, ttl);
				return;
			}
			else if (loadstate.equalsIgnoreCase("loading")) { //$NON-NLS-1$
				// if the process is already loading do nothing
				next = ttl - elapsed;
			}
			else if (loadstate.equalsIgnoreCase("loaded")) { //$NON-NLS-1$
				if (elapsed >= ttl) {
					runJob(vdb, table, ttl);
					return;
				} 
				next = ttl - elapsed;	
			}
			else if (loadstate.equalsIgnoreCase("failed_load")) { //$NON-NLS-1$
				if (elapsed > ttl/4 || elapsed > 60000) { // exceeds 1/4 of cached time or 5 mins
					runJob(vdb, table, ttl);
					return;
				}
				next = Math.min(((ttl/4)-elapsed), (60000-elapsed));
			}
			scheduleJob(vdb, table, ttl, next);
		}
	}	
	
	public ResultsFuture<?> executeAsynchQuery(VDBMetaData vdb, String command) throws SQLException {
		try {
			return DQPCore.executeQuery(command, vdb, "embedded-async", "internal", -1, getDQP(), new DQPCore.ResultsListener() { //$NON-NLS-1$ //$NON-NLS-2$
				@Override
				public void onResults(List<String> columns,
						List<? extends List<?>> results) throws Exception {
					
				}
			});
		} catch (Throwable e) {
			throw new SQLException(e);
		}
	}
	
    public List<Map<String, String>> executeQuery(VDBMetaData vdb, String command) throws SQLException {
        final List<Map<String, String>> rows = new ArrayList<Map<String,String>>();
        try {
        	Future<?> f = DQPCore.executeQuery(command, vdb, "embedded-async", "internal", -1, getDQP(), new DQPCore.ResultsListener() { //$NON-NLS-1$ //$NON-NLS-2$
                @Override
                public void onResults(List<String> columns, List<? extends List<?>> results) throws Exception {
                    for (List<?> row:results) {
                        TreeMap<String, String> rowResult = new TreeMap<String, String>();
                        for (int colNum = 0; colNum < columns.size(); colNum++) {
                            Object value = row.get(colNum);
                            if (value != null) {
                                if (value instanceof Timestamp) {
                                    value = ((Timestamp)value).getTime();
                                }
                            }
                            rowResult.put(columns.get(colNum), value == null?null:value.toString());
                        }
                        rows.add(rowResult);
                    }
                }
            }); 
        	f.get();
        } catch (InterruptedException e) {
        	//break
        	throw new TeiidRuntimeException(e); 
        } catch (ExecutionException e) {
        	if (e.getCause() != null) {
                throw new SQLException(e.getCause()); 
        	}
        	throw new SQLException(e);
        } catch (Throwable e) {
            throw new SQLException(e);
        }   
        return rows;
    }
		
	public abstract Executor getExecutor();
	public abstract ScheduledExecutorService getScheduledExecutorService();
	public abstract DQPCore getDQP();
}
