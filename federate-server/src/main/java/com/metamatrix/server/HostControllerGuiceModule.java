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
import org.jgroups.Channel;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.jboss.JBossCacheFactory;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.VMMessageBus;
import com.metamatrix.platform.registry.ClusteredRegistryState;

class HostControllerGuiceModule extends AbstractModule {

	Host host;
	String vmName;

	public HostControllerGuiceModule(Host host, String vmName) {
		this.host = host;
		this.vmName = vmName;
	}
	
	@Override
	protected void configure() {
	
		bindConstant().annotatedWith(Names.named(Configuration.HOSTNAME)).to(host.getFullName());
		bindConstant().annotatedWith(Names.named(Configuration.VMNAME)).to(vmName);
		bind(Host.class).annotatedWith(Names.named(Configuration.HOST)).toInstance(host);
				
		try {
			Names.bindProperties(binder(), CurrentConfiguration.getProperties());
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		
		bind(Channel.class).toProvider(JGroupsProvider.class).in(Scopes.SINGLETON);
		bind(Cache.class).toProvider(CacheProvider.class).in(Scopes.SINGLETON);
		bind(CacheFactory.class).to(JBossCacheFactory.class).in(Scopes.SINGLETON);
		bind(ClusteredRegistryState.class).in(Scopes.SINGLETON);
		bind(MessageBus.class).to(VMMessageBus.class).in(Scopes.SINGLETON); // VM Message bus is in common-internal
	}

}
