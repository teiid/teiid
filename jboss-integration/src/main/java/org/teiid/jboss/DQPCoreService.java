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
package org.teiid.jboss;

import java.io.Serializable;
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
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.LRUCache;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.AuthorizationValidator;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.internal.process.TransactionServerImpl;
import org.teiid.dqp.service.TransactionService;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.services.InternalEventDistributorFactory;
import org.teiid.vdb.runtime.VDBKey;


public class DQPCoreService extends DQPConfiguration implements Serializable, Service<DQPCore>  {
	private static final long serialVersionUID = -4676205340262775388L;
		
	private transient TransactionServerImpl transactionServerImpl = new TransactionServerImpl();
	private transient DQPCore dqpCore = new DQPCore();

	private final InjectedValue<WorkManager> workManagerInjector = new InjectedValue<WorkManager>();
	private final InjectedValue<XATerminator> xaTerminatorInjector = new InjectedValue<XATerminator>();
	private final InjectedValue<TransactionManager> txnManagerInjector = new InjectedValue<TransactionManager>();
	private final InjectedValue<BufferManager> bufferManagerInjector = new InjectedValue<BufferManager>();
	private final InjectedValue<TranslatorRepository> translatorRepositoryInjector = new InjectedValue<TranslatorRepository>();
	private final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
	private final InjectedValue<AuthorizationValidator> authorizationValidatorInjector = new InjectedValue<AuthorizationValidator>();
	private final InjectedValue<SessionAwareCache> preparedPlanCacheInjector = new InjectedValue<SessionAwareCache>();
	private final InjectedValue<SessionAwareCache> resultSetCacheInjector = new InjectedValue<SessionAwareCache>();
	private final InjectedValue<InternalEventDistributorFactory> eventDistributorFactoryInjector = new InjectedValue<InternalEventDistributorFactory>();
	
	@Override
    public void start(final StartContext context) {
		this.transactionServerImpl.setWorkManager(getWorkManagerInjector().getValue());
		this.transactionServerImpl.setXaTerminator(getXaTerminatorInjector().getValue());
		this.transactionServerImpl.setTransactionManager(getTxnManagerInjector().getValue());
		this.transactionServerImpl.setDetectTransactions(true);
		
		setAuthorizationValidator(authorizationValidatorInjector.getValue());
		this.dqpCore.setBufferManager(bufferManagerInjector.getValue());
		
		this.dqpCore.setTransactionService((TransactionService)LogManager.createLoggingProxy(LogConstants.CTX_TXN_LOG, transactionServerImpl, new Class[] {TransactionService.class}, MessageLevel.DETAIL, Thread.currentThread().getContextClassLoader()));
		this.dqpCore.setEventDistributor(getEventDistributorFactoryInjector().getValue().getReplicatedEventDistributor());
		this.dqpCore.setResultsetCache(getResultSetCacheInjector().getValue());
		this.dqpCore.setPreparedPlanCache(getPreparedPlanCacheInjector().getValue());
		this.dqpCore.start(this);

		
    	// add vdb life cycle listeners
    	getVdbRepository().addListener(new VDBLifeCycleListener() {
			
			private Set<VDBKey> recentlyRemoved = Collections.synchronizedSet(Collections.newSetFromMap(new LRUCache<VDBKey, Boolean>(10000)));
			
			@Override
			public void removed(String name, int version, CompositeVDB vdb) {
				recentlyRemoved.add(new VDBKey(name, version));
			}
			
			@Override
			public void added(String name, int version, CompositeVDB vdb, boolean reloading) {
				if (!recentlyRemoved.remove(new VDBKey(name, version))) {
					return;
				}
				// terminate all the previous sessions
		        List<ServiceName> services = context.getController().getServiceContainer().getServiceNames();
		        for (ServiceName service:services) {
		        	if (TeiidServiceNames.TRANSPORT_BASE.isParentOf(service)) {
		        		ServiceController<?> transport = context.getController().getServiceContainer().getService(service);
		        		if (transport != null) {
		        			TransportService t = TransportService.class.cast(transport.getValue());					
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
		        if (getResultSetCacheInjector().getValue() != null) {
		        	getResultSetCacheInjector().getValue().clearForVDB(name, version);
		        }
		        if (getPreparedPlanCacheInjector().getValue() != null) {
		        	getPreparedPlanCacheInjector().getValue().clearForVDB(name, version);
		        }
			}

			@Override
			public void finishedDeployment(String name, int version, CompositeVDB cvdb, boolean reloading) {
			}			
			
			@Override
			public void beforeRemove(String name, int version, CompositeVDB cvdb) {
			}
		}); 		

    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50001, this.dqpCore.getRuntimeVersion(), new Date(System.currentTimeMillis()).toString()));
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
    	
    	LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50002, new Date(System.currentTimeMillis()).toString())); 
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

	public InjectedValue<BufferManager> getBufferManagerInjector() {
		return bufferManagerInjector;
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

	public InjectedValue<InternalEventDistributorFactory> getEventDistributorFactoryInjector() {
		return eventDistributorFactoryInjector;
	}
}
