/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import org.jboss.cache.Cache;
import org.jboss.cache.DefaultCacheFactory;
import org.jboss.cache.config.Configuration;
import org.jgroups.Channel;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;

@Singleton
class CacheProvider implements Provider<org.jboss.cache.Cache> {

	@Inject
	ChannelProvider channelProvider;
	
	public Cache get() {
		Channel channel = this.channelProvider.get(ChannelProvider.ChannelID.CACHE);
		
		Cache cache = new DefaultCacheFactory().createCache("jboss-cache-configuration.xml", false); //$NON-NLS-1$
		Configuration config = cache.getConfiguration();
		config.getRuntimeConfig().setChannel(channel);
		config.setClusterName(channel.getClusterName());
		
		// if the channel is already in connected state then cache is not 
		// getting events about joining the cluster
		if (channel.isConnected()) {
			channel.disconnect();
		}
		return cache;
	}
}
