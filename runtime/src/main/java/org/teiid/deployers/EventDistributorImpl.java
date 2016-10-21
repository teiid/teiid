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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import javax.script.ScriptEngineManager;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.core.TeiidComponentException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ExecutionFactoryProvider;
import org.teiid.dqp.internal.process.DataTierManagerImpl;
import org.teiid.events.EventDistributor;
import org.teiid.events.EventListener;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.*;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.metadatastore.AdminAwareEventDistributor;
import org.teiid.query.metadata.DDLMetadataRepository;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.DatabaseUtil;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.VDBResources;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.DdlPlan;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

public abstract class EventDistributorImpl implements EventDistributor {
	private Set<EventListener> listeners = Collections.newSetFromMap(new ConcurrentHashMap<EventListener, Boolean>());
	private AdminAwareEventDistributor adminEventDistributor;
	
	public abstract VDBRepository getVdbRepository();
	public abstract ExecutionFactoryProvider getExecutionFactoryProvider();
	public abstract ConnectorManagerRepository getConnectorManagerRepository();
	public abstract Admin getAdmin();
	public abstract ClassLoader getClassLoader(String[] path) throws MetadataException;
	
	public EventDistributorImpl() {
		getVdbRepository().addListener(new VDBLifeCycleListener() {
			@Override
			public void removed(String name, CompositeVDB vdb) {
				for(EventListener el:EventDistributorImpl.this.listeners) {
					try {
						el.vdbUndeployed(name, vdb.getVDB().getVersion());
					} catch (Exception e) {
						LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40148, "undeployed", vdb.getVDBKey())); //$NON-NLS-1$
					}
				}
			}
			@Override
			public void finishedDeployment(String name, CompositeVDB vdb) {
				for(EventListener el:EventDistributorImpl.this.listeners) {
					try {					
						if (vdb.getVDB().getStatus().equals(Status.ACTIVE)) {
							el.vdbLoaded(vdb.getVDB());
						}
						else {
							el.vdbLoadFailed(vdb.getVDB());
						}
					} catch (Exception e) {
						LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40148, "finished deployment", vdb.getVDBKey())); //$NON-NLS-1$
					}
				}
			}
			@Override
			public void added(String name, CompositeVDB vdb) {
				for(EventListener el:EventDistributorImpl.this.listeners) {
					try {
						el.vdbDeployed(name, vdb.getVDB().getVersion());
					} catch (Exception e) {
						LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40148, "deployed", vdb.getVDBKey())); //$NON-NLS-1$
					}
				}
			}
			@Override
			public void beforeRemove(String name, CompositeVDB vdb) {
			}			
		});
		
		// add delegators
		adminEventDistributor = new AdminAwareEventDistributor(getAdmin());
	}
	
	@Override
	public void updateMatViewRow(String vdbName, int vdbVersion, String schema,
			String viewName, List<?> tuple, boolean delete) {
		updateMatViewRow(vdbName, String.valueOf(vdbVersion), schema, viewName, tuple, delete);
	}
	
	@Override
	public void updateMatViewRow(String vdbName, String vdbVersion, String schema,
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
	public void dataModification(String vdbName, int vdbVersion, String schema,
			String... tableNames) {
		dataModification(vdbName, String.valueOf(vdbVersion), schema, tableNames);
	}
	
	@Override
	public void dataModification(String vdbName, String vdbVersion, String schema,	String... tableNames) {
		updateModified(true, vdbName, vdbVersion, schema, tableNames);
	}
	
	private void updateModified(boolean data, String vdbName, String vdbVersion, String schema,
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
		setColumnStats(vdbName, String.valueOf(vdbVersion), schemaName, tableName, columnName, stats);
	}
	
	@Override
	public void setColumnStats(String vdbName, String vdbVersion,
			String schemaName, String tableName, String columnName,
			ColumnStats stats) {
	    VDBMetaData vdb = getVdbRepository().getLiveVDB(vdbName, vdbVersion);
        Table t = getTable(vdbName, vdbVersion, schemaName, tableName);
		if (t == null) {
			return;
		}
		Column c = t.getColumnByName(columnName);
		if (c != null) {
			DdlPlan.setColumnStats(vdb, c, stats);
		}
	}
	
	@Override
	public void setTableStats(String vdbName, int vdbVersion,
			String schemaName, String tableName, TableStats stats) {
		setTableStats(vdbName, String.valueOf(vdbVersion), schemaName, tableName, stats);
	}
	
	@Override
	public void setTableStats(String vdbName, String vdbVersion,
			String schemaName, String tableName, TableStats stats) {
	    VDBMetaData vdb = getVdbRepository().getLiveVDB(vdbName, vdbVersion);
		Table t = getTable(vdbName, vdbVersion, schemaName, tableName);
		if (t == null) {
			return;
		}
		DdlPlan.setTableStats(vdb, t, stats);
	}

	private Table getTable(String vdbName, String vdbVersion, String schemaName,
			String tableName) {
		Schema s = getSchema(vdbName, vdbVersion, schemaName);
		if (s == null) {
			return null;
		}
		return s.getTables().get(tableName);
	}

	private Schema getSchema(String vdbName, String vdbVersion, String schemaName) {
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
		setInsteadOfTriggerDefinition(vdbName, String.valueOf(vdbVersion), schema, viewName, triggerEvent, triggerDefinition, enabled);
	}
	
	@Override
	public void setInsteadOfTriggerDefinition(String vdbName, String vdbVersion,
			String schema, String viewName, TriggerEvent triggerEvent,
			String triggerDefinition, Boolean enabled) {
		Table t = getTable(vdbName, vdbVersion, schema, viewName);
		if (t == null) {
			return;
		}
		DdlPlan.alterInsteadOfTrigger(getVdbRepository().getLiveVDB(vdbName, vdbVersion), t, triggerDefinition, enabled, triggerEvent, true);
	}
	
	@Override
	public void setProcedureDefinition(String vdbName, int vdbVersion,
			String schema, String procName, String definition) {
		setProcedureDefinition(vdbName, String.valueOf(vdbVersion), schema, procName, definition);
	}
	
	@Override
	public void setProcedureDefinition(String vdbName, String vdbVersion,String schema, String procName, String definition) {
		Schema s = getSchema(vdbName, vdbVersion, schema);
		if (s == null) {
			return;
		}
		Procedure p = s.getProcedures().get(procName);
		if (p == null) {
			return;
		}
		DdlPlan.alterProcedureDefinition(getVdbRepository().getLiveVDB(vdbName, vdbVersion), p, definition, true);
	}
	
	@Override
	public void setViewDefinition(String vdbName, int vdbVersion,
			String schema, String viewName, String definition) {
		setViewDefinition(vdbName, String.valueOf(vdbVersion), schema, viewName, definition);
	}
	
	@Override
	public void setViewDefinition(String vdbName, String vdbVersion, String schema, String viewName, String definition) {
		Table t = getTable(vdbName, vdbVersion, schema, viewName);
		if (t == null) {
			return;
		}
		DdlPlan.alterView(getVdbRepository().getLiveVDB(vdbName, vdbVersion), t, definition, true);
	}
	
	@Override
	public void setProperty(String vdbName, int vdbVersion, String uuid,
			String name, String value) {
		setProperty(vdbName, String.valueOf(vdbVersion), uuid, name, value);
	}
	
	@Override
	public void setProperty(String vdbName, String vdbVersion, String uuid,
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
		    DdlPlan.setProperty(vdb, record, name, value);
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

    @Override
    public void dropDatabase(Database database) {
        for (Server s : database.getServers()) {
            dropServer(database.getName(), database.getVersion(), s);
        }
        for (DataWrapper dw : database.getDataWrappers()) {
            dropDataWrapper(database.getName(), database.getVersion(), dw.getName(), dw.getType() != null);
        }
        getVdbRepository().removeVDB(database.getName(), database.getVersion());
    }

    @Override
    public void reloadDatabase(Database database) {
        getVdbRepository().removeVDB(database.getName(), database.getVersion());
        createDatabase(database);
    }
    
    @Override
    public void createDatabase(Database database) {
        VDBMetaData vdb = DatabaseUtil.convert(database);
        vdb.addProperty("database-origin", "db-store");
        getVdbRepository().addPendingDeployment(vdb);
        MetadataStore store = new MetadataStore();
        try {
            boolean hasMetadata = false;
            for (ModelMetaData model : vdb.getModelMetaDatas().values()) {
                Schema schema = database.getSchema(model.getName());
                if (!schema.getTables().isEmpty() || !schema.getProcedures().isEmpty()
                        || !schema.getFunctions().isEmpty()) {
                    String ddl = DDLStringVisitor.getDDLString(database.getSchema(model.getName()), null, null);
                    DDLMetadataRepository repo = new DDLMetadataRepository();
                    MetadataFactory mf = new MetadataFactory(database.getName(), database.getVersion(),
                            getVdbRepository().getRuntimeTypeMap(), model);
                    // for thread safety each factory gets it's own instance.
                    mf.setParser(new QueryParser()); 
                    repo.loadMetadata(mf, null, null, ddl);
                    mf.mergeInto(store);
                    hasMetadata = true;
                }
            }     
        
            // avoid kicking off materialization scripts
            if (!hasMetadata) {
                vdb.addProperty("load-matviews", "false");
            }

            // handle all the classloading of libraries here.
            UDFMetaData udf = new UDFMetaData();
            if (vdb.getPropertyValue("lib") != null) {
                ClassLoader classLoader = getClassLoader(parsePath(vdb.getPropertyValue("lib")));
                if (classLoader != null) {
                    vdb.addAttchment(ClassLoader.class, classLoader);
                    vdb.addAttchment(ScriptEngineManager.class, new ScriptEngineManager(classLoader));
                }
                udf.setFunctionClassLoader(classLoader);
            }
            getVdbRepository().addVDB(vdb, store, new LinkedHashMap<String, VDBResources.Resource>(), udf,
                    getConnectorManagerRepository());
        } catch (VirtualDatabaseException e) {
            throw new MetadataException(e);
        } catch (TranslatorException e) {
            throw new MetadataException(e);
        }
        getVdbRepository().finishDeployment(database.getName(), database.getVersion());
    }
    
    private String[] parsePath(String path) {
        ArrayList<String> list = new ArrayList<String>();
        StringTokenizer st = new StringTokenizer(path, ",");
        while(st.hasMoreTokens()) {
            String token = st.nextToken().trim();
            list.add(token);
        }
        return list.toArray(new String[list.size()]);
    }    
    
    @Override
    public void createDataWrapper(String dbName, String version, DataWrapper dataWrapper) {
        this.adminEventDistributor.createDataWrapper(dbName, version, dataWrapper);
        
        // handle override translators
        if (dataWrapper.getType() != null) {
            try {
                // create translator here
                VDBTranslatorMetaData translator = new VDBTranslatorMetaData();
                translator.setName(dataWrapper.getName());
                if (dataWrapper.getType() != null) {
                    ExecutionFactoryProvider efp = getConnectorManagerRepository().getProvider();
                    ExecutionFactory<Object, Object> parent = efp.getExecutionFactory(dataWrapper.getType());
                    translator.setExecutionFactoryClass(parent.getClass());
                }
                String module = dataWrapper.getProperty("module", false);
                if (module != null) {
                    translator.setModuleName(module);
                }
                if (dataWrapper.getAnnotation() != null) {
                    translator.setDescription(dataWrapper.getAnnotation());
                }
                for (String key:dataWrapper.getProperties().keySet()) {
                    if (key.equalsIgnoreCase("module")) {
                        continue;
                    }
                    translator.addProperty(key, dataWrapper.getProperties().get(key));
                }
                getConnectorManagerRepository().getProvider().addOverrideTranslator(translator);
            } catch (ConnectorManagerException e) {
                throw new MetadataException(e);
            }
        }
    }
    
    @Override
    public void dropDataWrapper(String dbName, String version, String dataWrapperName, boolean override) {
        if (override) {
            try {
                this.adminEventDistributor.dropDataWrapper(dbName, version, dataWrapperName, override);
                getConnectorManagerRepository().getProvider().removeOverrideTranslator(dataWrapperName);
            } catch (ConnectorManagerException e) {
                throw new MetadataException(e);
            }
        }
    }
    
    @Override
    public void createServer(String dbName, String version, Server server) {
        try {
            this.adminEventDistributor.createServer(dbName, version, server);
            String jndiName = server.getJndiName();
            if (jndiName == null) {
                jndiName = server.getName();
            }
            getConnectorManagerRepository().createConnectorManager(server.getName(), server.getDataWrapper(),
                    jndiName, getExecutionFactoryProvider(), false);
        } catch (ConnectorManagerException e) {
            throw new MetadataException(e);
        }                
    }
    @Override
    public void dropServer(String dbName, String version, Server server) {
        this.adminEventDistributor.dropServer(dbName, version, server);
        getConnectorManagerRepository().removeConnectorManager(server.getName());
    }
   
}
