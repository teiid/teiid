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

import java.util.ArrayList;
import java.util.List;

import javax.transaction.TransactionManager;

import org.teiid.cache.CacheFactory;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.DataRolePolicyDecider;
import org.teiid.dqp.internal.process.DefaultAuthorizationValidator;
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
    private boolean useDisk = true;
    private String bufferDirectory;
    private CacheFactory cacheFactory;
    private int maxResultSetCacheStaleness = DEFAULT_MAX_STALENESS_SECONDS;
    private String infinispanConfigFile;
    private List<SocketConfiguration> transports;
    private int maxODBCLobSizeAllowed = 5*1024*1024; // 5 MB
    private int maxAsyncThreads = DEFAULT_MAX_ASYNC_WORKERS;

    private boolean allowEnvFunction;

    //buffer manager properties
    private int processorBatchSize = -1 ;
    private int maxReserveKb = -1;
    private int maxProcessingKb = -1;
    private boolean inlineLobs = true;
    private int maxOpenFiles = -1;
    private long maxBufferSpace = -1;
    private long maxFileSize = -1;
    private boolean encryptFiles = false;
    private int maxStorageObjectSize = -1;
    private boolean memoryBufferOffHeap = false;
    private int memoryBufferSpace = -1;

    private String nodeName;

    private AuthenticationType authenticationType;

    public EmbeddedConfiguration() {
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
     * on the Teiid contexts.
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

    @Deprecated
    public String getInfinispanConfigFile() {
        return infinispanConfigFile;
    }

    /**
     * @see #setCacheFactory(CacheFactory) to set the {@link CacheFactory} directly
     */
    @Deprecated
    public void setInfinispanConfigFile(String infinispanConfigFile) {
        this.infinispanConfigFile = infinispanConfigFile;
    }

    public CacheFactory getCacheFactory() {
        return this.cacheFactory;
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

    protected void stop() {
        if (cacheFactory != null) {
            cacheFactory.destroy();
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

    @Deprecated
    public int getMaxReserveKb() {
        return maxReserveKb;
    }

    @Deprecated
    public void setMaxReserveKb(int maxReserveKb) {
        this.maxReserveKb = maxReserveKb;
    }

    @Deprecated
    public int getMaxProcessingKb() {
        return maxProcessingKb;
    }

    @Deprecated
    public void setMaxProcessingKb(int maxProcessingKb) {
        this.maxProcessingKb = maxProcessingKb;
    }

    @Deprecated
    public boolean isInlineLobs() {
        return inlineLobs;
    }

    @Deprecated
    public void setInlineLobs(boolean inlineLobs) {
        this.inlineLobs = inlineLobs;
    }

    @Deprecated
    public int getMaxOpenFiles() {
        return maxOpenFiles;
    }

    @Deprecated
    public void setMaxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
    }

    @Deprecated
    public long getMaxBufferSpace() {
        return maxBufferSpace;
    }

    @Deprecated
    public void setMaxBufferSpace(long maxBufferSpace) {
        this.maxBufferSpace = maxBufferSpace;
    }

    @Deprecated
    public long getMaxFileSize() {
        return maxFileSize;
    }

    @Deprecated
    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    @Deprecated
    public boolean isEncryptFiles() {
        return encryptFiles;
    }

    @Deprecated
    public void setEncryptFiles(boolean encryptFiles) {
        this.encryptFiles = encryptFiles;
    }

    @Deprecated
    public int getMaxStorageObjectSize() {
        return maxStorageObjectSize;
    }

    @Deprecated
    public void setMaxStorageObjectSize(int maxStorageObjectSize) {
        this.maxStorageObjectSize = maxStorageObjectSize;
    }

    @Deprecated
    public boolean isMemoryBufferOffHeap() {
        return memoryBufferOffHeap;
    }

    @Deprecated
    public void setMemoryBufferOffHeap(boolean memoryBufferOffHeap) {
        this.memoryBufferOffHeap = memoryBufferOffHeap;
    }

    @Deprecated
    public int getMemoryBufferSpace() {
        return memoryBufferSpace;
    }

    @Deprecated
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
