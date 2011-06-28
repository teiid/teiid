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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.jboss.deployers.spi.DeploymentException;
import org.mockito.Mockito;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.cache.CacheConfiguration.Policy;
import org.teiid.client.DQP;
import org.teiid.client.security.ILogon;
import org.teiid.deployers.MetadataStoreGroup;
import org.teiid.deployers.UDFMetaData;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.FakeTransactionService;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.service.FakeBufferService;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.metadata.TransformationMetadata.Resource;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.services.SessionServiceImpl;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.ClientServiceRegistryImpl;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.LogonImpl;

@SuppressWarnings({"nls", "serial"})
public class FakeServer extends ClientServiceRegistryImpl implements ConnectionProfile {

	SessionServiceImpl sessionService = new SessionServiceImpl();
	LogonImpl logon;
	DQPCore dqp = new DQPCore();
	VDBRepository repo = new VDBRepository();
	private ConnectorManagerRepository cmr;
	private boolean useCallingThread = true;
	
	public FakeServer() {
		this(new DQPConfiguration());
	}
	
	public FakeServer(DQPConfiguration config) {
		this.logon = new LogonImpl(sessionService, null);
		
		this.repo.setSystemStore(VDBMetadataFactory.getSystem());
		this.repo.setSystemFunctionManager(new SystemFunctionManager());
		this.repo.odbcEnabled();
		this.repo.start();
		
        this.sessionService.setVDBRepository(repo);
        this.dqp.setBufferService(new FakeBufferService());
        this.dqp.setCacheFactory(new DefaultCacheFactory());
        this.dqp.setTransactionService(new FakeTransactionService());
        
        cmr = Mockito.mock(ConnectorManagerRepository.class);
        Mockito.stub(cmr.getConnectorManager("source")).toReturn(new ConnectorManager("x", "x") {
        	@Override
        	public SourceCapabilities getCapabilities() {
        		return new BasicSourceCapabilities();
        	}
        });
        
        config.setResultsetCacheConfig(new CacheConfiguration(Policy.LRU, 60, 250, "resultsetcache")); //$NON-NLS-1$
        this.dqp.setCacheFactory(new DefaultCacheFactory() {
        	@Override
        	public boolean isReplicated() {
        		return true; //pretend to be replicated for matview tests
        	}
        });
        this.dqp.start(config);
        this.sessionService.setDqp(this.dqp);
        
        registerClientService(ILogon.class, logon, null);
        registerClientService(DQP.class, dqp, null);
	}
	
	public void setConnectorManagerRepository(ConnectorManagerRepository cmr) {
		this.cmr = cmr;
	}
	
	public void stop() {
		this.dqp.stop();
	}
	
	public void setMetadataRepository(MetadataRepository metadataRepository) {
		this.repo.setMetadataRepository(metadataRepository);
		this.dqp.setMetadataRepository(metadataRepository);
	}
	
	public void setUseCallingThread(boolean useCallingThread) {
		this.useCallingThread = useCallingThread;
	}

	public void deployVDB(String vdbName, String vdbPath) throws Exception {
		deployVDB(vdbName, vdbPath, null);
	}
	
	public void deployVDB(String vdbName, String vdbPath, Map<String, Collection<FunctionMethod>> udfs) throws Exception {
		IndexMetadataFactory imf = VDBMetadataFactory.loadMetadata(new File(vdbPath).toURI().toURL());
		MetadataStore metadata = imf.getMetadataStore(repo.getSystemStore().getDatatypes());
		LinkedHashMap<String, Resource> entries = imf.getEntriesPlusVisibilities();
        deployVDB(vdbName, metadata, entries, udfs);		
	}
	
	public void deployVDB(String vdbName, MetadataStore metadata,
			LinkedHashMap<String, Resource> entries) {
		deployVDB(vdbName, metadata, entries, null);
	}

	public void deployVDB(String vdbName, MetadataStore metadata,
			LinkedHashMap<String, Resource> entries, Map<String, Collection<FunctionMethod>> udfs) {
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
        	UDFMetaData udfMetaData = null;
        	if (udfs != null) {
        		udfMetaData = new UDFMetaData();
        		for (Map.Entry<String, Collection<FunctionMethod>> entry : udfs.entrySet()) {
        			udfMetaData.addFunctions(entry.getKey(), entry.getValue());
        		}
        	}
			this.repo.addVDB(vdbMetaData, stores, entries, udfMetaData, cmr);
			this.repo.finishDeployment(vdbName, 1);
		} catch (DeploymentException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void removeVDB(String vdbName) {
		this.repo.removeVDB(vdbName, 1);
	}

	private void addModel(VDBMetaData vdbMetaData, Schema schema) {
		ModelMetaData model = new ModelMetaData();
		model.setName(schema.getName());
		vdbMetaData.addModel(model);
		model.addSourceMapping("source", "translator", "jndi:source");
	}
	
	public VDBMetaData getVDB(String vdbName) {
		return this.repo.getVDB(vdbName, 1);
	}
	
	public void undeployVDB(String vdbName) {
		this.repo.removeVDB(vdbName, 1);
	}
	
	public void mergeVDBS(String sourceVDB, String targetVDB) throws AdminException {
		this.repo.mergeVDBs(sourceVDB, 1, targetVDB, 1);
	}
	
	public ConnectionImpl createConnection(String embeddedURL) throws Exception {
		final Properties p = new Properties();
		TeiidDriver.parseURL(embeddedURL, p);

		return connect(embeddedURL, p);
	}
	
	@Override
	public ConnectionImpl connect(String url, Properties info)
			throws TeiidSQLException {
		LocalServerConnection conn;
		try {
			conn = new LocalServerConnection(info, useCallingThread) {
				@Override
				protected ClientServiceRegistry getClientServiceRegistry() {
					return FakeServer.this;
				}
			};
		} catch (CommunicationException e) {
			throw TeiidSQLException.create(e);
		} catch (ConnectionException e) {
			throw TeiidSQLException.create(e);
		}
		return new ConnectionImpl(conn, info, url);	
	}
	
}
