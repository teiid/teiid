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
package org.teiid.jboss.deployers;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.LRUCache;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.AuthorizationValidator;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DataTierManagerImpl;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.internal.process.TransactionServerImpl;
import org.teiid.dqp.service.BufferService;
import org.teiid.dqp.service.TransactionService;
import org.teiid.events.EventDistributor;
import org.teiid.events.EventDistributorFactory;
import org.teiid.jboss.IntegrationPlugin;
import org.teiid.jboss.TeiidServiceNames;
import org.teiid.jboss.Transport;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnStats;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.metadata.TableStats;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.processor.DdlPlan;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.services.BufferServiceImpl;
import org.teiid.transport.LocalServerConnection;
import org.teiid.vdb.runtime.VDBKey;


public class RuntimeEngineDeployer extends DQPConfiguration implements Serializable, EventDistributor, EventDistributorFactory, Service<DQPCore>  {
	private static final long serialVersionUID = -4676205340262775388L;
		
	private transient TransactionServerImpl transactionServerImpl = new TransactionServerImpl();
	private transient DQPCore dqpCore = new DQPCore();
	private transient EventDistributor eventDistributor;	
	private transient EventDistributor eventDistributorProxy;

	private final InjectedValue<WorkManager> workManagerInjector = new InjectedValue<WorkManager>();
	private final InjectedValue<XATerminator> xaTerminatorInjector = new InjectedValue<XATerminator>();
	private final InjectedValue<TransactionManager> txnManagerInjector = new InjectedValue<TransactionManager>();
	private final InjectedValue<BufferServiceImpl> bufferServiceInjector = new InjectedValue<BufferServiceImpl>();
	private final InjectedValue<TranslatorRepository> translatorRepositoryInjector = new InjectedValue<TranslatorRepository>();
	private final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
	private final InjectedValue<AuthorizationValidator> authorizationValidatorInjector = new InjectedValue<AuthorizationValidator>();
	private final InjectedValue<SessionAwareCache> preparedPlanCacheInjector = new InjectedValue<SessionAwareCache>();
	private final InjectedValue<SessionAwareCache> resultSetCacheInjector = new InjectedValue<SessionAwareCache>();
	private final InjectedValue<ObjectReplicator> objectReplicatorInjector = new InjectedValue<ObjectReplicator>();
	
	@Override
    public void start(final StartContext context) {
		this.transactionServerImpl.setWorkManager(getWorkManagerInjector().getValue());
		this.transactionServerImpl.setXaTerminator(getXaTerminatorInjector().getValue());
		this.transactionServerImpl.setTransactionManager(getTxnManagerInjector().getValue());
		
		setAuthorizationValidator(authorizationValidatorInjector.getValue());
		
		setBufferService(bufferServiceInjector.getValue());
		
		dqpCore.setTransactionService((TransactionService)LogManager.createLoggingProxy(LogConstants.CTX_TXN_LOG, transactionServerImpl, new Class[] {TransactionService.class}, MessageLevel.DETAIL, Thread.currentThread().getContextClassLoader()));

		if (getObjectReplicatorInjector().getValue() != null) {
			try {
				this.eventDistributor = getObjectReplicatorInjector().getValue().replicate(LocalServerConnection.TEIID_RUNTIME_CONTEXT, EventDistributor.class, this, 0);
			} catch (Exception e) {
				LogManager.logError(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.getString("replication_failed", this)); //$NON-NLS-1$
			}
		}
		else {
			LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("distributed_cache_not_enabled")); //$NON-NLS-1$
		}
		
		this.dqpCore.setMetadataRepository(getVdbRepository().getMetadataRepository());
		this.dqpCore.setEventDistributor(this.eventDistributor);
		this.dqpCore.setResultsetCache(getResultSetCacheInjector().getValue());
		this.dqpCore.setPreparedPlanCache(getPreparedPlanCacheInjector().getValue());
		this.dqpCore.start(this);
		this.eventDistributorProxy = (EventDistributor)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {EventDistributor.class}, new InvocationHandler() {
			
			@Override
			public Object invoke(Object proxy, Method method, Object[] args)
					throws Throwable {
				method.invoke(RuntimeEngineDeployer.this, args);
				if (eventDistributor != null) {
					method.invoke(eventDistributor, args);
				}
				return null;
			}
		});
		
    	// add vdb life cycle listeners
    	getVdbRepository().addListener(new VDBLifeCycleListener() {
			
			private Set<VDBKey> recentlyRemoved = Collections.newSetFromMap(new LRUCache<VDBKey, Boolean>(10000));
			
			@Override
			public void removed(String name, int version, CompositeVDB vdb) {
				recentlyRemoved.add(new VDBKey(name, version));
			}
			
			@Override
			public void added(String name, int version, CompositeVDB vdb) {
				if (!recentlyRemoved.remove(new VDBKey(name, version))) {
					return;
				}
				// terminate all the previous sessions
		        List<ServiceName> services = context.getController().getServiceContainer().getServiceNames();
		        for (ServiceName service:services) {
		        	if (TeiidServiceNames.TRANSPORT_BASE.isParentOf(service)) {
		        		ServiceController<?> transport = context.getController().getServiceContainer().getService(service);
		        		if (transport != null) {
		        			Transport t = Transport.class.cast(transport.getValue());					
		        			Collection<SessionMetadata> sessions = t.getActiveSessions();
							for (SessionMetadata session:sessions) {
								if (name.equalsIgnoreCase(session.getVDBName()) && version == session.getVDBVersion()){
									t.terminateSession(session.getSessionId());
								}
							}
		        		}
		        	}
		        }
			        
				// dump the caches. 
				getResultSetCacheInjector().getValue().clearForVDB(name, version);
				getPreparedPlanCacheInjector().getValue().clearForVDB(name, version);
			}			
		}); 		

    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("engine_started", this.dqpCore.getRuntimeVersion(), new Date(System.currentTimeMillis()).toString())); //$NON-NLS-1$
	}	
	
	@Override
	public DQPCore getValue() throws IllegalStateException, IllegalArgumentException {
		return this.dqpCore;
	}
    
	@Override
    public void stop(StopContext context) {
    	try {
	    	this.dqpCore.stop();
    	} catch(TeiidRuntimeException e) {
    		// this bean is already shutdown
    	}
    	
    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("engine_stopped", new Date(System.currentTimeMillis()).toString())); //$NON-NLS-1$
    	
    	if (getObjectReplicatorInjector().getValue() != null && this.eventDistributor != null) {
    		getObjectReplicatorInjector().getValue().stop(this.eventDistributor);
    	}
    }
    
	public void setBufferService(BufferService service) {
		this.dqpCore.setBufferService(service);
	}
	
	
	@Override
	public void updateMatViewRow(String vdbName, int vdbVersion, String schema,
			String viewName, List<?> tuple, boolean delete) {
		VDBMetaData metadata = getVdbRepository().getVDB(vdbName, vdbVersion);
		if (metadata != null) {
			GlobalTableStore gts = metadata.getAttachment(GlobalTableStore.class);
			if (gts != null) {
				try {
					gts.updateMatViewRow((RelationalPlanner.MAT_PREFIX + schema + '.' + viewName).toUpperCase(), tuple, delete);
				} catch (TeiidComponentException e) {
					LogManager.logError(LogConstants.CTX_RUNTIME, e, IntegrationPlugin.Util.getString("replication_failed", "updateMatViewRow")); //$NON-NLS-1$ //$NON-NLS-2$
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
			Table table = s.getTables().get(name.toUpperCase());
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
		for (Column c : t.getColumns()) {
			if (c.getName().equalsIgnoreCase(columnName)) {
				c.setColumnStats(stats);
				t.setLastModified(System.currentTimeMillis());
				break;
			}
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
		return s.getTables().get(tableName.toUpperCase());
	}

	private Schema getSchema(String vdbName, int vdbVersion, String schemaName) {
		VDBMetaData vdb = getVdbRepository().getVDB(vdbName, vdbVersion);
		if (vdb == null) {
			return null;
		}
		TransformationMetadata tm = vdb.getAttachment(TransformationMetadata.class);
		if (tm == null) {
			return null;
		}
		return tm.getMetadataStore().getSchemas().get(schemaName.toUpperCase());
	}
	
	@Override
	public void setInsteadOfTriggerDefinition(String vdbName, int vdbVersion,
			String schema, String viewName, TriggerEvent triggerEvent,
			String triggerDefinition, Boolean enabled) {
		Table t = getTable(vdbName, vdbVersion, schema, viewName);
		if (t == null) {
			return;
		}
		DdlPlan.alterInsteadOfTrigger(getVdbRepository().getVDB(vdbName, vdbVersion), t, triggerDefinition, enabled, triggerEvent);
	}
	
	@Override
	public void setProcedureDefinition(String vdbName, int vdbVersion,String schema, String procName, String definition) {
		Schema s = getSchema(vdbName, vdbVersion, schema);
		if (s == null) {
			return;
		}
		Procedure p = s.getProcedures().get(procName.toUpperCase());
		if (p == null) {
			return;
		}
		DdlPlan.alterProcedureDefinition(getVdbRepository().getVDB(vdbName, vdbVersion), p, definition);
	}
	
	@Override
	public void setViewDefinition(String vdbName, int vdbVersion, String schema, String viewName, String definition) {
		Table t = getTable(vdbName, vdbVersion, schema, viewName);
		if (t == null) {
			return;
		}
		DdlPlan.alterView(getVdbRepository().getVDB(vdbName, vdbVersion), t, definition);
	}
	
	@Override
	public void setProperty(String vdbName, int vdbVersion, String uuid,
			String name, String value) {
		VDBMetaData vdb = getVdbRepository().getVDB(vdbName, vdbVersion);
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
	public EventDistributor getEventDistributor() {
		return this.eventDistributorProxy;
	}
	
	public InjectedValue<SessionAwareCache> getResultSetCacheInjector() {
		return resultSetCacheInjector;
	}
	
	public InjectedValue<SessionAwareCache> getPreparedPlanCacheInjector() {
		return preparedPlanCacheInjector;
	}	

	public InjectedValue<TranslatorRepository> getTranslatorRepositoryInjector() {
		return translatorRepositoryInjector;
	}

	public InjectedValue<VDBRepository> getVdbRepositoryInjector() {
		return vdbRepositoryInjector;
	}
	
	private VDBRepository getVdbRepository() {
		return vdbRepositoryInjector.getValue();
	}	

	public InjectedValue<AuthorizationValidator> getAuthorizationValidatorInjector() {
		return authorizationValidatorInjector;
	}

	public InjectedValue<BufferServiceImpl> getBufferServiceInjector() {
		return bufferServiceInjector;
	}

	public InjectedValue<TransactionManager> getTxnManagerInjector() {
		return txnManagerInjector;
	}

	public InjectedValue<XATerminator> getXaTerminatorInjector() {
		return xaTerminatorInjector;
	}

	public InjectedValue<WorkManager> getWorkManagerInjector() {
		return workManagerInjector;
	}

	public InjectedValue<ObjectReplicator> getObjectReplicatorInjector() {
		return objectReplicatorInjector;
	}
}
