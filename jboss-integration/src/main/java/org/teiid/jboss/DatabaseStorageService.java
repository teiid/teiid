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

import java.util.concurrent.Executor;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadatastore.DefaultDatabaseStore;
import org.teiid.query.metadata.DDLProcessor;
import org.teiid.query.metadata.DatabaseStorage;
import org.teiid.services.InternalEventDistributorFactory;

class DatabaseStorageService implements Service<DDLProcessor> {
    protected final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
	protected final InjectedValue<InternalEventDistributorFactory> eventDistributorFactoryInjector = new InjectedValue<InternalEventDistributorFactory>();
	protected final InjectedValue<TranslatorRepository> translatorRepositoryInjector = new InjectedValue<TranslatorRepository>();
	protected final InjectedValue<Executor> executorInjector = new InjectedValue<Executor>();
	
	private DatabaseStorage storage;
	private ConnectorManagerRepository connectorManagerRepo;
	private final JBossLifeCycleListener lifeCycleListener;
	private DefaultDatabaseStore store;
	
	public DatabaseStorageService(DatabaseStorage storage, DefaultDatabaseStore store, JBossLifeCycleListener lifeCycleListener) {
	    this.storage = storage;
	    this.store = store;
	    this.lifeCycleListener = lifeCycleListener;
	}
	
	@Override
	public void start(StartContext context) throws StartException {
        store.setVDBRepository(vdbRepositoryInjector.getValue());
        
        store.setExecutionFactoryProvider(this.connectorManagerRepo.getProvider());
        store.setConnectorManagerRepository(this.connectorManagerRepo);
        store.setFunctionalStore(eventDistributorFactoryInjector.getValue().getEventDistributor());
        
        //TODO: this is hack, we need to find a better way to see when the server is completely started
        this.executorInjector.getValue().execute(new Runnable() {
            @Override
            public void run() {
                boolean started = true;
                int i = 0;
                while (!lifeCycleListener.isStarted() && i++ < 30) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        started = false;
                        break;
                    }
                }
                if (started) {
                    storage.load(store);
                } else {
                    LogManager.logDetail(LogConstants.CTX_RUNTIME, 
                            IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50112));
                }
            }
        });
	}

	@Override
	public void stop(StopContext context) {
	}

	@Override
	public DDLProcessor getValue() throws IllegalStateException, IllegalArgumentException {
		return this.store;
	}

    public void setConnectorManagerRepo(ConnectorManagerRepository connectorManagerRepo) {
        this.connectorManagerRepo = connectorManagerRepo;   
    }
}
