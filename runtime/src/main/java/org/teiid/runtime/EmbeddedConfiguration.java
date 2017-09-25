/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.runtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.teiid.cache.CacheFactory;
import org.teiid.cache.infinispan.InfinispanCacheFactory;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.DataRolePolicyDecider;
import org.teiid.dqp.internal.process.DefaultAuthorizationValidator;
import org.teiid.dqp.internal.process.TeiidExecutor;
import org.teiid.dqp.internal.process.ThreadReuseExecutor;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.query.ObjectReplicator;
import org.teiid.security.SecurityHelper;
import org.teiid.transport.SocketConfiguration;

public class EmbeddedConfiguration extends DQPConfiguration {
	
	static final int DEFAULT_MAX_ASYNC_WORKERS = 10;
	private SecurityHelper securityHelper;
	private String securityDomain;
	private TransactionManager transactionManager;
	private ObjectReplicator objectReplicator;
	private WorkManager workManager;
	private boolean useDisk = true;
	private String bufferDirectory;
	private CacheFactory cacheFactory;
	private int maxResultSetCacheStaleness = DEFAULT_MAX_STALENESS_SECONDS;
	private String infinispanConfigFile = "infinispan-config.xml"; //$NON-NLS-1$
	private String jgroupsConfigFile; // from infinispan-core
	private List<SocketConfiguration> transports;
	private int maxODBCLobSizeAllowed = 5*1024*1024; // 5 MB
	private int maxAsyncThreads = DEFAULT_MAX_ASYNC_WORKERS;
	
	private boolean allowEnvFunction;
	
	private int processorBatchSize ;
	private int maxReserveKb ;
	private int maxProcessingKb ;
	private boolean inlineLobs = true;
	private int maxOpenFiles ;
	
	private long maxBufferSpace ;
	private long maxFileSize ;
	private boolean encryptFiles = false;
	private int maxStorageObjectSize ;
	private boolean memoryBufferOffHeap = false;
	private int memoryBufferSpace ;
	private String nodeName;
	
    private DefaultCacheManager cacheManager;
	private AuthenticationType authenticationType;
	
	public EmbeddedConfiguration() {
		processorBatchSize = -1;
		maxReserveKb = -1;
		maxProcessingKb = -1;
		maxOpenFiles = -1;
		maxBufferSpace = -1;
		maxFileSize = -1;
		maxStorageObjectSize = -1;
		memoryBufferSpace = -1;
		
		DefaultAuthorizationValidator authorizationValidator = new DefaultAuthorizationValidator();
		authorizationValidator.setPolicyDecider(new DataRolePolicyDecider());
		this.setAuthorizationValidator(authorizationValidator);
	}
	
	public SecurityHelper getSecurityHelper() {
		return securityHelper;
	}
	/**
	 * Set the {@link SecurityHelper} that can associate the appropriate SecurityContext
	 * with threads executing Teiid tasks.  Will also set the appropriate user/subject information
	 * on the Teiid contexts. Not required if a {@link WorkManager} is set.
	 * 
	 * @param securityHelper
	 */
	public void setSecurityHelper(SecurityHelper securityHelper) {
		this.securityHelper = securityHelper;
	}
	public String getSecurityDomain() {
		return this.securityDomain;
	}
	public void setSecurityDomain(String securityDomain) {
		this.securityDomain = securityDomain;
	}
	public TransactionManager getTransactionManager() {
		return transactionManager;
	}
	public void setTransactionManager(TransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}
	
	public ObjectReplicator getObjectReplicator() {
		return objectReplicator;
	}
	
	public void setObjectReplicator(ObjectReplicator objectReplicator) {
		this.objectReplicator = objectReplicator;
	}
	
	/**
	 * Sets the {@link WorkManager} to be used instead of a {@link ThreadReuseExecutor}.
	 * This means that Teiid will not own the processing threads and will not necessarily be
	 * responsible for security context propagation.
	 * @param workManager
	 */
	public void setWorkManager(WorkManager workManager) {
		this.workManager = workManager;
	}
	public WorkManager getWorkManager() {
		return workManager;
	}
	
	@Override
	public TeiidExecutor getTeiidExecutor() {
		if (workManager == null) {
			return super.getTeiidExecutor();
		}
		//TODO: if concurrency is 1, then use a direct executor
		//the only scheduled task right now just restarts a workitem,
		//so that can be done in the scheduler thread
		return new WorkManagerTeiidExecutor(workManager);
	}
	
	public boolean isUseDisk() {
		return useDisk;
	}
	
	public void setUseDisk(boolean useDisk) {
		this.useDisk = useDisk;
	}
	
	public void setBufferDirectory(String dir) {
		this.bufferDirectory = dir;
	}
	
	public String getBufferDirectory() {
		return this.bufferDirectory;
	}
	
	public String getInfinispanConfigFile() {
		return infinispanConfigFile;
	}
	
	public void setInfinispanConfigFile(String infinispanConfigFile) {
		this.infinispanConfigFile = infinispanConfigFile;
	}
	
	public CacheFactory getCacheFactory() {
		if (this.cacheFactory == null) {
			try {
				cacheManager = new DefaultCacheManager(this.infinispanConfigFile, true);				
				for(String cacheName:cacheManager.getCacheNames()) {
	                if (getTransactionManager() != null) {
	                    setCacheTransactionManger(cacheName);
	                }				    
					cacheManager.startCache(cacheName);
				}
				this.cacheFactory = new InfinispanCacheFactory(cacheManager, this.getClass().getClassLoader());
			} catch (IOException e) {
				throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40100, e);
			}
		}
		return this.cacheFactory;
	}

    private void setCacheTransactionManger(String cacheName) {
        cacheManager.getCacheConfiguration(cacheName).transaction().transactionManagerLookup(new TransactionManagerLookup() {
            @Override
            public TransactionManager getTransactionManager() throws Exception {
                return EmbeddedConfiguration.this.getTransactionManager();
            }
        });
    }
	
	public void setCacheFactory(CacheFactory cacheFactory) {
		this.cacheFactory = cacheFactory;
	}
	public int getMaxResultSetCacheStaleness() {
		return maxResultSetCacheStaleness;
	}
	public void setMaxResultSetCacheStaleness(int maxResultSetCacheStaleness) {
		this.maxResultSetCacheStaleness = maxResultSetCacheStaleness;
	}
	public String getJgroupsConfigFile() {
		return jgroupsConfigFile;
	}
	public void setJgroupsConfigFile(String jgroupsConfigFile) {
		this.jgroupsConfigFile = jgroupsConfigFile;
	}	
	
	protected void stop() {
		if (cacheManager != null) {
			cacheManager.stop();
		}
	}
	
	public void addTransport(SocketConfiguration configuration) {
		if (this.transports == null) {
			this.transports = new ArrayList<SocketConfiguration>();
		}
		this.transports.add(configuration);
	}
	
	public List<SocketConfiguration> getTransports(){
		return this.transports;
	}
	
	public int getMaxODBCLobSizeAllowed() {
		return this.maxODBCLobSizeAllowed;
	}
	
	public void setMaxODBCLobSizeAllowed(int lobSize) {
		this.maxODBCLobSizeAllowed = lobSize;
	}	
	
    public int getMaxAsyncThreads() {
        return maxAsyncThreads;
    }

    public void setMaxAsyncThreads(int maxAsyncThreads) {
        this.maxAsyncThreads = maxAsyncThreads;
    }

	public int getProcessorBatchSize() {
		return processorBatchSize;
	}

	public void setProcessorBatchSize(int processorBatchSize) {
		this.processorBatchSize = processorBatchSize;
	}

	public int getMaxReserveKb() {
		return maxReserveKb;
	}

	public void setMaxReserveKb(int maxReserveKb) {
		this.maxReserveKb = maxReserveKb;
	}

	public int getMaxProcessingKb() {
		return maxProcessingKb;
	}

	public void setMaxProcessingKb(int maxProcessingKb) {
		this.maxProcessingKb = maxProcessingKb;
	}

	public boolean isInlineLobs() {
		return inlineLobs;
	}

	public void setInlineLobs(boolean inlineLobs) {
		this.inlineLobs = inlineLobs;
	}

	public int getMaxOpenFiles() {
		return maxOpenFiles;
	}

	public void setMaxOpenFiles(int maxOpenFiles) {
		this.maxOpenFiles = maxOpenFiles;
	}

	public long getMaxBufferSpace() {
		return maxBufferSpace;
	}

	public void setMaxBufferSpace(long maxBufferSpace) {
		this.maxBufferSpace = maxBufferSpace;
	}

	public long getMaxFileSize() {
		return maxFileSize;
	}

	public void setMaxFileSize(long maxFileSize) {
		this.maxFileSize = maxFileSize;
	}

	public boolean isEncryptFiles() {
		return encryptFiles;
	}

	public void setEncryptFiles(boolean encryptFiles) {
		this.encryptFiles = encryptFiles;
	}

	public int getMaxStorageObjectSize() {
		return maxStorageObjectSize;
	}

	public void setMaxStorageObjectSize(int maxStorageObjectSize) {
		this.maxStorageObjectSize = maxStorageObjectSize;
	}

	public boolean isMemoryBufferOffHeap() {
		return memoryBufferOffHeap;
	}

	public void setMemoryBufferOffHeap(boolean memoryBufferOffHeap) {
		this.memoryBufferOffHeap = memoryBufferOffHeap;
	}

	public int getMemoryBufferSpace() {
		return memoryBufferSpace;
	}

	public void setMemoryBufferSpace(int memoryBufferSpace) {
		this.memoryBufferSpace = memoryBufferSpace;
	}
	
	public AuthenticationType getAuthenticationType() {
		return this.authenticationType;
	}
	
	public void setAuthenticationType(AuthenticationType authenticationType) {
		this.authenticationType = authenticationType;
	}

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public boolean isAllowEnvFunction() {
        return allowEnvFunction;
    }
    
    public void setAllowEnvFunction(boolean allowEnvFunction) {
        this.allowEnvFunction = allowEnvFunction;
    }
}
