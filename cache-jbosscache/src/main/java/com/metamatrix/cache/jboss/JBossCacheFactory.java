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

package com.metamatrix.cache.jboss;

import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.OperationsException;

import org.jboss.cache.Fqn;
import org.jboss.cache.Node;
import org.jboss.cache.Region;
import org.jboss.cache.config.EvictionPolicyConfig;
import org.jboss.cache.eviction.FIFOConfiguration;
import org.jboss.cache.eviction.LFUConfiguration;
import org.jboss.cache.eviction.LRUConfiguration;
import org.jboss.cache.eviction.MRUConfiguration;
import org.jboss.cache.jmx.CacheJmxWrapper;
import org.jboss.cache.jmx.CacheJmxWrapperMBean;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.metamatrix.cache.Cache;
import com.metamatrix.cache.CacheConfiguration;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.Cache.Type;
import com.metamatrix.cache.CacheConfiguration.Policy;
import com.metamatrix.core.MetaMatrixRuntimeException;

@Singleton
public class JBossCacheFactory implements CacheFactory {
	private org.jboss.cache.Cache cacheStore;
	private volatile boolean destroyed = false;
	private ObjectName jmxName;
	
	@Inject
	public JBossCacheFactory(org.jboss.cache.Cache cacheStore) {
		try {
			CacheJmxWrapperMBean wrapper = new CacheJmxWrapper(cacheStore);			
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			this.jmxName = new ObjectName("Teiid:service=JBossCache,name=cache"); //$NON-NLS-1$
			mbs.registerMBean(wrapper, this.jmxName);
			wrapper.create();
			wrapper.start();
			this.cacheStore =  wrapper.getCache();
		} catch (OperationsException e) {
			throw new MetaMatrixRuntimeException(e);
		}  catch (MBeanRegistrationException e) {
			throw new MetaMatrixRuntimeException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public Cache get(Type type, CacheConfiguration config) {
		if (!destroyed) {
			Node cacheRoot = this.cacheStore.getRoot().addChild(Fqn.fromString("Teiid")); //$NON-NLS-1$
			Node node = cacheRoot.addChild(Fqn.fromString(type.location()));
			
			
			Region cacheRegion = this.cacheStore.getRegion(node.getFqn(), true);
			cacheRegion.setEvictionPolicy(buildEvictionPolicy(config));
			
			return new JBossCache(this.cacheStore, node.getFqn());
		}
		throw new MetaMatrixRuntimeException("Cache system has been shutdown"); //$NON-NLS-1$
	}

	private EvictionPolicyConfig buildEvictionPolicy(CacheConfiguration config) {
		EvictionPolicyConfig evictionConfig = null;
		
		if (config.getPolicy() == Policy.LRU) {
			LRUConfiguration lru = new LRUConfiguration();
			lru.setMaxNodes(config.getMaxNodes());
			lru.setMaxAgeSeconds(config.getMaxAgeInSeconds());
			lru.setTimeToLiveSeconds(0);
			evictionConfig = lru;
		}
		else if (config.getPolicy() == Policy.MRU) {
			MRUConfiguration mru = new MRUConfiguration();
			mru.setMaxNodes(config.getMaxNodes());
			evictionConfig = mru;
		}
		else if (config.getPolicy() == Policy.FIFO) {
			FIFOConfiguration fifo = new FIFOConfiguration();
			fifo.setMaxNodes(config.getMaxNodes());
			evictionConfig = fifo;
		}
		else if (config.getPolicy() == Policy.LFU) {
			LFUConfiguration lfu  = new LFUConfiguration();
			lfu.setMaxNodes(config.getMaxNodes());
			evictionConfig = lfu;
		}
		return evictionConfig;
	}    
	
	public void destroy() {
		try {
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			mbs.unregisterMBean(this.jmxName);
		} catch (InstanceNotFoundException e) {
		} catch (MBeanRegistrationException e) {
		} finally {
			this.cacheStore.destroy();
			this.destroyed = true;
		}
	}	
	
}
