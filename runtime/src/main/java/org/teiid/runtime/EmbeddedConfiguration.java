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

package org.teiid.runtime;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;

import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;

import org.infinispan.manager.DefaultCacheManager;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.teiid.cache.CacheFactory;
import org.teiid.cache.infinispan.InfinispanCacheFactory;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.TeiidExecutor;
import org.teiid.dqp.internal.process.ThreadReuseExecutor;
import org.teiid.query.ObjectReplicator;
import org.teiid.replication.jgroups.ChannelFactory;
import org.teiid.replication.jgroups.JGroupsObjectReplicator;
import org.teiid.security.SecurityHelper;

public class EmbeddedConfiguration extends DQPConfiguration {
	
	private SecurityHelper securityHelper;
	private List<String> securityDomains;
	private TransactionManager transactionManager;
	private ObjectReplicator objectReplicator;
	private WorkManager workManager;
	private boolean useDisk = true;
	private String bufferDirectory;
	private CacheFactory cacheFactory;
	private int maxResultSetCacheStaleness = 60;
	private String infinispanConfigFile = "infinispan-config.xml"; //$NON-NLS-1$
	private String jgroupsConfigFile = "tcp.xml"; //$NON-NLS-1$
	
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
	public List<String> getSecurityDomains() {
		return securityDomains;
	}
	public void setSecurityDomains(List<String> securityDomains) {
		this.securityDomains = securityDomains;
	}
	public TransactionManager getTransactionManager() {
		return transactionManager;
	}
	public void setTransactionManager(TransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}
	
	public ObjectReplicator getObjectReplicator() {
		if (this.objectReplicator == null) {
			this.objectReplicator = new JGroupsObjectReplicator(new ChannelFactory() {
				@Override
				public Channel createChannel(String id) throws Exception {
					return new JChannel(this.getClass().getClassLoader().getResource(getJgroupsConfigFile()));
				}
			}, Executors.newCachedThreadPool());			
		}
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
				DefaultCacheManager manager = new DefaultCacheManager(this.infinispanConfigFile, true);
				manager.startCaches(manager.getCacheNames().toArray(new String[manager.getCacheNames().size()]));
				this.cacheFactory = new InfinispanCacheFactory(manager, this.getClass().getClassLoader());
			} catch (IOException e) {
				throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40100, e);
			}
		}
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
	public String getJgroupsConfigFile() {
		return jgroupsConfigFile;
	}
	public void setJgroupsConfigFile(String jgroupsConfigFile) {
		this.jgroupsConfigFile = jgroupsConfigFile;
	}	
}
