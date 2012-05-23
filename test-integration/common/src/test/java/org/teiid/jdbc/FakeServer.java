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
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.mockito.Mockito;
import org.teiid.Replicated;
import org.teiid.Replicated.ReplicationMode;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBImportMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.cache.Cache;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.cache.CacheConfiguration.Policy;
import org.teiid.client.DQP;
import org.teiid.client.security.ILogon;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.CompositeVDB;
import org.teiid.deployers.UDFMetaData;
import org.teiid.deployers.VDBLifeCycleListener;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.FakeTransactionService;
import org.teiid.dqp.internal.process.CachedResults;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.service.BufferService;
import org.teiid.dqp.service.FakeBufferService;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.index.IndexMetadataStore;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.query.ObjectReplicator;
import org.teiid.query.ReplicatedObject;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.tempdata.GlobalTableStore;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
import org.teiid.security.Credentials;
import org.teiid.security.SecurityHelper;
import org.teiid.services.BufferServiceImpl;
import org.teiid.services.SessionServiceImpl;
import org.teiid.services.TeiidLoginContext;
import org.teiid.transport.ClientServiceRegistry;
import org.teiid.transport.ClientServiceRegistryImpl;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.LogonImpl;

@SuppressWarnings({"nls"})
public class FakeServer extends ClientServiceRegistryImpl implements ConnectionProfile {
	
	public static class DeployVDBParameter {
		public Map<String, Collection<FunctionMethod>> udfs;
		public MetadataRepository metadataRepo;
		public List<VDBImportMetadata> vdbImports;

		public DeployVDBParameter(Map<String, Collection<FunctionMethod>> udfs,
				MetadataRepository metadataRepo) {
			this.udfs = udfs;
			this.metadataRepo = metadataRepo;
		}
	}
	
	public interface ReplicatedCache<K, V> extends Cache<K, V>  {
		
		@Replicated(replicateState=ReplicationMode.PULL)
		public V get(K key);

		@Replicated(replicateState=ReplicationMode.PUSH)
		V put(K key, V value, Long ttl);
			
		@Replicated()
		V remove(K key);
		
	}
	
	public static class ReplicatedCacheImpl<K extends Serializable, V> implements ReplicatedCache<K, V>, ReplicatedObject<K> {
		private Cache<K, V> cache;
		
		public ReplicatedCacheImpl(Cache<K, V> cache) {
			this.cache = cache;
		}

		public void clear() {
			cache.clear();
		}

		public V get(K key) {
			return cache.get(key);
		}

		public String getName() {
			return cache.getName();
		}

		public Set<K> keys() {
			return cache.keys();
		}

		public V put(K key, V value, Long ttl) {
			return cache.put(key, value, ttl);
		}

		public V remove(K key) {
			return cache.remove(key);
		}

		public int size() {
			return cache.size();
		}
		
		@Override
		public void getState(K stateId, OutputStream ostream) {
			V value = get(stateId);
			if (value != null) {
				try {
					ObjectOutputStream oos = new ObjectOutputStream(ostream);
					oos.writeObject(value);
					oos.close();
				} catch (IOException e) {
					throw new TeiidRuntimeException(e);
				}
			}
		}
		
		@Override
		public void setState(K stateId, InputStream istream) {
			try {
				ObjectInputStream ois = new ObjectInputStream(istream);
				V value = (V) ois.readObject();
				this.put(stateId, value, null);
			} catch (IOException e) {
				throw new TeiidRuntimeException(e);
			} catch (ClassNotFoundException e) {
				throw new TeiidRuntimeException(e);
			}
		}

		@Override
		public boolean hasState(K stateId) {
			return cache.get(stateId) != null;
		}

		@Override
		public void droppedMembers(Collection<Serializable> addresses) {
		}

		@Override
		public void getState(OutputStream ostream) {
		}

		@Override
		public void setAddress(Serializable address) {
		}

		@Override
		public void setState(InputStream istream) {
		}

		
	}

	SessionServiceImpl sessionService = new SessionServiceImpl() {
		@Override
		protected TeiidLoginContext authenticate(String userName,
				Credentials credentials, String applicationName,
				List<String> domains, boolean onlyallowPassthrough)
				throws LoginException {
			return new TeiidLoginContext(userName+"@"+domains.get(0), new Subject(), domains.get(0), new Object());
		}
		
	};
	LogonImpl logon;
	DQPCore dqp = new DQPCore();
	VDBRepository repo = new VDBRepository();
	private ConnectorManagerRepository cmr;
	private boolean useCallingThread = true;
	private ObjectReplicator replicator;
	
	public FakeServer() {
		this(new DQPConfiguration());
	}
	
	public void setReplicator(ObjectReplicator replicator) {
		this.replicator = replicator;
	}
	
	public FakeServer(DQPConfiguration config) {
		start(config, false);
	}

	public FakeServer(boolean start) {
		if (start) {
			start(new DQPConfiguration(), false);
		}
	}

	@SuppressWarnings("serial")
	public void start(DQPConfiguration config, boolean realBufferMangaer) {
		sessionService.setSecurityHelper(Mockito.mock(SecurityHelper.class));
		sessionService.setSecurityDomains(Arrays.asList("somedomain"));
		
		this.logon = new LogonImpl(sessionService, null);
		this.repo.addListener(new VDBLifeCycleListener() {
			
			@Override
			public void added(String name, int version,
					CompositeVDB vdb) {
				
			}
			
			@Override
			public void removed(String name, int version, CompositeVDB vdb) {
				
			}
			
			@Override
			public void finishedDeployment(String name, int version, CompositeVDB vdb) {
				GlobalTableStore gts = new GlobalTableStoreImpl(dqp.getBufferManager(), vdb.getVDB().getAttachment(TransformationMetadata.class));
				if (replicator != null) {
					try {
						gts = replicator.replicate(name + version, GlobalTableStore.class, gts, 300000);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				vdb.getVDB().addAttchment(GlobalTableStore.class, gts);
			}
		});
		this.repo.setSystemStore(VDBMetadataFactory.getSystem());
		this.repo.setSystemFunctionManager(new SystemFunctionManager());
		this.repo.odbcEnabled();
		this.repo.start();
		
        this.sessionService.setVDBRepository(repo);
        BufferService bs = null;
        if (!realBufferMangaer) {
        	bs = new FakeBufferService(false);
        } else {
        	BufferServiceImpl bsi = new BufferServiceImpl();
        	bsi.setDiskDirectory(UnitTestUtil.getTestScratchPath());
        	bsi.start();
        	bs = bsi;
        }
        if (replicator != null) {
			try {
				final TupleBufferCache tbc = replicator.replicate("$BM$", TupleBufferCache.class, bs.getBufferManager(), 0);
				bs = new FakeBufferService(bs.getBufferManager(), tbc);
			} catch (Exception e) {
				throw new TeiidRuntimeException(e);
			}
        }
        this.dqp.setBufferManager(bs.getBufferManager());
    	
        //TODO: wire in an infinispan cluster rather than this dummy replicated cache
        DefaultCacheFactory dcf = new DefaultCacheFactory() {
        	public boolean isReplicated() {
        		return true;
        	}
        	
        	@Override
        	public <K, V> Cache<K, V> get(String location,
        			CacheConfiguration config) {
        		Cache<K, V> result = super.get(location, config);
        		if (replicator != null) {
        			try {
						return replicator.replicate("$RS$", ReplicatedCache.class, new ReplicatedCacheImpl(result), 0);
					} catch (Exception e) {
						throw new TeiidRuntimeException(e);
					}
        		}
        		return result;
        	}
        };
		SessionAwareCache rs = new SessionAwareCache<CachedResults>(dcf, SessionAwareCache.Type.RESULTSET, new CacheConfiguration(Policy.LRU, 60, 250, "resultsetcache"));
		SessionAwareCache ppc = new SessionAwareCache<PreparedPlan>(dcf, SessionAwareCache.Type.PREPAREDPLAN, new CacheConfiguration());
        rs.setTupleBufferCache(bs.getTupleBufferCache());
        this.dqp.setResultsetCache(rs);
        
        ppc.setTupleBufferCache(bs.getTupleBufferCache());
        this.dqp.setPreparedPlanCache(ppc);		
        
        this.dqp.setTransactionService(new FakeTransactionService());
        
        cmr = new ConnectorManagerRepository() {
        	@Override
        	public ConnectorManager getConnectorManager(String connectorName) {
        		ConnectorManager cm = super.getConnectorManager(connectorName);
        		if (cm != null) {
        			return cm;
        		}
        		if (connectorName.equalsIgnoreCase("source")) {
        			return new ConnectorManager("x", "x") {
        	        	@Override
        	        	public SourceCapabilities getCapabilities() {
        	        		return new BasicSourceCapabilities();
        	        	}
        			};
        		}
        		return null;
        	}
        };
        
        this.dqp.start(config);
        this.sessionService.setDqp(this.dqp);
        
        registerClientService(ILogon.class, logon, null);
        registerClientService(DQP.class, dqp, null);
	}
	
	public DQPCore getDqp() {
		return dqp;
	}
	
	public ConnectorManagerRepository getConnectorManagerRepository() {
		return cmr;
	}
	
	public void setConnectorManagerRepository(ConnectorManagerRepository cmr) {
		this.cmr = cmr;
	}
	
	public void stop() {
		this.dqp.stop();
	}
	
	public void setUseCallingThread(boolean useCallingThread) {
		this.useCallingThread = useCallingThread;
	}
	
	public void deployVDB(String vdbName, String vdbPath) throws Exception {
        deployVDB(vdbName, vdbPath, new DeployVDBParameter(null, null));		
	}	

	public void deployVDB(String vdbName, String vdbPath, DeployVDBParameter parameterObject) throws Exception {
		IndexMetadataStore imf = VDBMetadataFactory.loadMetadata(vdbName, new File(vdbPath).toURI().toURL());
        deployVDB(vdbName, imf, parameterObject);		
	}
	
	public void deployVDB(String vdbName, MetadataStore metadata) {
		deployVDB(vdbName, metadata, new DeployVDBParameter(null, null));
	}

	public void deployVDB(String vdbName, MetadataStore metadata, DeployVDBParameter parameterObject) {
		VDBMetaData vdbMetaData = new VDBMetaData();
        vdbMetaData.setName(vdbName);
        vdbMetaData.setStatus(VDB.Status.ACTIVE);
        
        for (Schema schema : metadata.getSchemas().values()) {
        	ModelMetaData model = addModel(vdbMetaData, schema);
        	if (parameterObject.metadataRepo != null) {
        		model.addAttchment(MetadataRepository.class, parameterObject.metadataRepo);
        	}
        }
                        
        try {
        	UDFMetaData udfMetaData = null;
        	if (parameterObject.udfs != null) {
        		udfMetaData = new UDFMetaData();
        		for (Map.Entry<String, Collection<FunctionMethod>> entry : parameterObject.udfs.entrySet()) {
        			udfMetaData.addFunctions(entry.getKey(), entry.getValue());
        		}
        	}
        	
        	if (parameterObject.vdbImports != null) {
        		for (VDBImportMetadata vdbImport : parameterObject.vdbImports) {
					vdbMetaData.getVDBImports().add(vdbImport);
				}
        	}
        	
			this.repo.addVDB(vdbMetaData, metadata, (metadata instanceof IndexMetadataStore)?((IndexMetadataStore)metadata).getEntriesPlusVisibilities():null, udfMetaData, cmr);
			this.repo.finishDeployment(vdbMetaData.getName(), vdbMetaData.getVersion());
			this.repo.getVDB(vdbMetaData.getName(), vdbMetaData.getVersion()).setStatus(VDB.Status.ACTIVE);
		} catch (VirtualDatabaseException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void removeVDB(String vdbName) {
		this.repo.removeVDB(vdbName, 1);
	}

	private ModelMetaData addModel(VDBMetaData vdbMetaData, Schema schema) {
		ModelMetaData model = new ModelMetaData();
		model.setName(schema.getName());
		vdbMetaData.addModel(model);
		model.addSourceMapping("source", "translator", "jndi:source");
		return model;
	}
	
	public VDBMetaData getVDB(String vdbName) {
		return this.repo.getVDB(vdbName, 1);
	}
	
	public void undeployVDB(String vdbName) {
		this.repo.removeVDB(vdbName, 1);
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
