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

package com.metamatrix.server;

import java.util.List;
import java.util.Properties;

import org.jboss.cache.Cache;
import org.jboss.cache.DefaultCacheFactory;
import org.jboss.cache.config.Configuration;
import org.jboss.cache.config.CacheLoaderConfig.IndividualCacheLoaderConfig;
import org.jgroups.Channel;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.common.config.CurrentConfiguration;

@Singleton
class CacheProvider implements Provider<org.jboss.cache.Cache> {

	@Inject
	ChannelProvider channelProvider;
	
	@Inject
	@Named(com.metamatrix.server.Configuration.CLUSTERNAME)
	String channelName;
	
	public Cache get() {
		Channel channel = this.channelProvider.get(ChannelProvider.ChannelID.CACHE);
		
		Cache cache = new DefaultCacheFactory().createCache("jboss-cache-configuration.xml", false); //$NON-NLS-1$
		Configuration config = cache.getConfiguration();
		List<IndividualCacheLoaderConfig> configs = config.getCacheLoaderConfig().getIndividualCacheLoaderConfigs();
		Properties p = configs.get(0).getProperties();
		p.setProperty("location", CurrentConfiguration.getInstance().getDefaultHost().getDataDirectory() + "/cache"); //$NON-NLS-1$ //$NON-NLS-2$
		configs.get(0).setProperties(p);
		config.getRuntimeConfig().setChannel(channel);
		config.setClusterName(this.channelName);
		
		// if the channel is already in connected state then cache is not 
		// getting events about joining the cluster
		if (channel.isConnected()) {
			channel.disconnect();
		}
		return cache;
	}
}
