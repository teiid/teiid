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
import java.util.Date;

import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.PreParser;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.AuthorizationValidator;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.internal.process.TransactionServerImpl;
import org.teiid.dqp.service.SessionService;
import org.teiid.dqp.service.TransactionService;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.metadata.DatabaseStorage;
import org.teiid.services.InternalEventDistributorFactory;


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
	private final InjectedValue<PreParser> preParserInjector = new InjectedValue<PreParser>();
	private final InjectedValue<SessionAwareCache> preparedPlanCacheInjector = new InjectedValue<SessionAwareCache>();
	private final InjectedValue<SessionAwareCache> resultSetCacheInjector = new InjectedValue<SessionAwareCache>();
	private final InjectedValue<InternalEventDistributorFactory> eventDistributorFactoryInjector = new InjectedValue<InternalEventDistributorFactory>();
	private final InjectedValue<DatabaseStorage> databaseStorageInjector = new InjectedValue<DatabaseStorage>();
	
	@Override
    public void start(final StartContext context) {
		this.transactionServerImpl.setWorkManager(getWorkManagerInjector().getValue());
		this.transactionServerImpl.setXaTerminator(getXaTerminatorInjector().getValue());
		this.transactionServerImpl.setTransactionManager(getTxnManagerInjector().getValue());
		this.transactionServerImpl.setDetectTransactions(true);
		setPreParser(preParserInjector.getValue());
		setAuthorizationValidator(authorizationValidatorInjector.getValue());
		this.dqpCore.setBufferManager(bufferManagerInjector.getValue());
		
		this.dqpCore.setTransactionService((TransactionService)LogManager.createLoggingProxy(LogConstants.CTX_TXN_LOG, transactionServerImpl, new Class[] {TransactionService.class}, MessageLevel.DETAIL, Thread.currentThread().getContextClassLoader()));
		this.dqpCore.setEventDistributor(getEventDistributorFactoryInjector().getValue().getReplicatedEventDistributor());
		this.dqpCore.setResultsetCache(getResultSetCacheInjector().getValue());
		this.dqpCore.setPreparedPlanCache(getPreparedPlanCacheInjector().getValue());
		this.dqpCore.setDatabaseStorage(getDatabaseStorageInjector().getValue());
		this.dqpCore.start(this);

		
    	// add vdb life cycle listeners
    	getVdbRepository().addListener(new VDBLifeCycleListener() {
			
			@Override
			public void removed(String name, CompositeVDB vdb) {
			    // terminate all the previous sessions
				SessionService sessionService = (SessionService) context.getController().getServiceContainer().getService(TeiidServiceNames.SESSION).getValue();
    			Collection<SessionMetadata> sessions = sessionService.getSessionsLoggedInToVDB(vdb.getVDBKey());
				for (SessionMetadata session:sessions) {
			        sessionService.terminateSession(session.getSessionId(), null);
				}
			        
				// dump the caches. 
				try {
			        SessionAwareCache<?> value = getResultSetCacheInjector().getValue();
					if (value != null) {
			        	value.clearForVDB(vdb.getVDBKey());
			        }
			        value = getPreparedPlanCacheInjector().getValue();
					if (value != null) {
			        	value.clearForVDB(vdb.getVDBKey());
			        }
				} catch (IllegalStateException e) {
					//already shutdown
				}
			}
			
			@Override
			public void added(String name, CompositeVDB vdb) {		    
			}

			@Override
			public void finishedDeployment(String name, CompositeVDB cvdb) {
                SessionService sessionService = (SessionService) context.getController().getServiceContainer().getService(TeiidServiceNames.SESSION).getValue();
                Collection<SessionMetadata> sessions = sessionService.getSessionsLoggedInToVDB(cvdb.getVDBKey());
                for (SessionMetadata session:sessions) {
                    session.setVdb(cvdb.getVDB());
                }   			    
			}			
			
			@Override
			public void beforeRemove(String name, CompositeVDB cvdb) {
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
	
	public InjectedValue<PreParser> getPreParserInjector() {
		return preParserInjector;
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
	
	public InjectedValue<DatabaseStorage> getDatabaseStorageInjector() {
		return databaseStorageInjector;
	}	
}
