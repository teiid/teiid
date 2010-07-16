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
package org.teiid.jdbc;

import java.io.File;
import java.util.Properties;

import org.jboss.deployers.spi.DeploymentException;
import org.mockito.Mockito;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.DQP;
import org.teiid.client.security.ILogon;
import org.teiid.deployers.MetadataStoreGroup;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.impl.FakeTransactionService;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.service.FakeBufferService;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.services.SessionServiceImpl;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.ClientServiceRegistryImpl;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.LogonImpl;

@SuppressWarnings("nls")
public class FakeServer extends ClientServiceRegistryImpl {

	SessionServiceImpl sessionService = new SessionServiceImpl();
	LogonImpl logon;
	DQPCore dqp = new DQPCore();
	VDBRepository repo = new VDBRepository();
	
	public FakeServer() {
		this.logon = new LogonImpl(sessionService, null);
		
		this.repo.setSystemStore(VDBMetadataFactory.getSystem());
		this.repo.odbcEnabled();
		this.repo.start();
		
        this.sessionService.setVDBRepository(repo);
        this.dqp.setBufferService(new FakeBufferService());
        this.dqp.setTransactionService(new FakeTransactionService());
        
        ConnectorManagerRepository cmr = Mockito.mock(ConnectorManagerRepository.class);
        Mockito.stub(cmr.getConnectorManager("source")).toReturn(new ConnectorManager("x", "x") {
        	@Override
        	public SourceCapabilities getCapabilities() {
        		return new BasicSourceCapabilities();
        	}
        });
        
        this.dqp.setConnectorManagerRepository(cmr);
        this.dqp.start(new DQPConfiguration());
        this.sessionService.setDqp(this.dqp);
        
        registerClientService(ILogon.class, logon, null);
        registerClientService(DQP.class, dqp, null);
	}
	
	public void deployVDB(String vdbName, String vdbPath) throws Exception {
		
		IndexMetadataFactory imf = VDBMetadataFactory.loadMetadata(new File(vdbPath).toURI().toURL());
		MetadataStore metadata = imf.getMetadataStore(repo.getSystemStore().getDatatypes());
		
        VDBMetaData vdbMetaData = new VDBMetaData();
        vdbMetaData.setName(vdbName);
        vdbMetaData.setStatus(VDB.Status.ACTIVE);
        
        for (Schema schema : repo.getSystemStore().getSchemas().values()) {
        	addModel(vdbMetaData, schema); 
        }
        
        for (Schema schema : repo.getODBCStore().getSchemas().values()) {
        	addModel(vdbMetaData, schema); 
        }        
        
        for (Schema schema : metadata.getSchemas().values()) {
        	addModel(vdbMetaData, schema); 
        }
                        
        try {
        	MetadataStoreGroup stores = new MetadataStoreGroup();
        	stores.addStore(metadata);
			this.repo.addVDB(vdbMetaData, stores, imf.getEntriesPlusVisibilities(), null);
		} catch (DeploymentException e) {
			throw new RuntimeException(e);
		}		
	}

	private void addModel(VDBMetaData vdbMetaData, Schema schema) {
		ModelMetaData model = new ModelMetaData();
		model.setName(schema.getName());
		vdbMetaData.addModel(model);
		model.addSourceMapping("source", "translator", "jndi:source");
	}
	
	public void undeployVDB(String vdbName) {
		this.repo.removeVDB(vdbName, 1);
	}
	
	public void mergeVDBS(String sourceVDB, String targetVDB) throws AdminException {
		this.repo.mergeVDBs(sourceVDB, 1, targetVDB, 1);
	}
	
	public ConnectionImpl createConnection(String embeddedURL) throws Exception {
		final Properties p = new Properties();
		EmbeddedProfile.parseURL(embeddedURL, p);

		return new ConnectionImpl(new LocalServerConnection(p) {
			@Override
			protected ClientServiceRegistry getClientServiceRegistry() {
				return FakeServer.this;
			}
		}, p, embeddedURL);
	}
	
	
}
