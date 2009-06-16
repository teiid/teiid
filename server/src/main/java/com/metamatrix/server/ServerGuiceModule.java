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

import org.jboss.cache.Cache;
import org.jgroups.mux.Multiplexer;
import org.teiid.dqp.internal.cache.DQPContextCache;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.jboss.JBossCacheFactory;
import com.metamatrix.common.comm.platform.socket.SocketVMController;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.jgroups.JGroupsMessageBus;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.ProcessMonitor;
import com.metamatrix.platform.service.proxy.ProxyManager;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.platform.vm.api.controller.ProcessManagement;
import com.metamatrix.platform.vm.controller.ServerEvents;

class ServerGuiceModule extends AbstractModule {
	
	Host host;
	String processName;

	public ServerGuiceModule(Host host, String processName) {
		this.host = host;
		this.processName = processName;
	}
	
	@Override
	protected void configure() {
		
		String systemName = null;
		try {
		    systemName = CurrentConfiguration.getInstance().getClusterName();
		} catch (ConfigurationException err) {
		    systemName = "Teiid-Cluster"; //$NON-NLS-1$
		}
		
		bindConstant().annotatedWith(Names.named(Configuration.HOSTNAME)).to(host.getFullName());
		bindConstant().annotatedWith(Names.named(Configuration.PROCESSNAME)).to(processName);
		bind(Host.class).annotatedWith(Names.named(Configuration.HOST)).toInstance(host);
		bindConstant().annotatedWith(Names.named(Configuration.CLUSTERNAME)).to(systemName);
		bindConstant().annotatedWith(Names.named(Configuration.LOGFILE)).to(StringUtil.replaceAll(host.getFullName(), ".", "_")+"_"+this.processName+".log"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		bindConstant().annotatedWith(Names.named(Configuration.LOGDIR)).to(host.getLogDirectory());
		bindConstant().annotatedWith(Names.named(Configuration.UNICAST_PORT)).to(getUnicastClusterPort());
		
		Names.bindProperties(binder(), CurrentConfiguration.getInstance().getProperties());
		
		bind(Multiplexer.class).toProvider(JGroupsProvider.class).in(Scopes.SINGLETON);
		bind(ChannelProvider.class).in(Scopes.SINGLETON);
		bind(Cache.class).toProvider(CacheProvider.class).in(Scopes.SINGLETON);
		bind(CacheFactory.class).to(JBossCacheFactory.class).in(Scopes.SINGLETON);
		bind(ClusteredRegistryState.class).in(Scopes.SINGLETON);
		bind(ProxyManager.class).in(Scopes.SINGLETON);
		bind(MessageBus.class).to(JGroupsMessageBus.class).in(Scopes.SINGLETON); // VM Message bus is in common-internal
		bind(ProcessManagement.class).to(SocketVMController.class).in(Scopes.SINGLETON);
		bind(ServerEvents.class).to(ProcessMonitor.class).in(Scopes.SINGLETON);
		bind(HostManagement.class).toProvider(HostManagementProvider.class).in(Scopes.SINGLETON);
		bind(DQPContextCache.class).in(Scopes.SINGLETON);
		
		// this needs to be removed.
		binder().requestStaticInjection(PlatformProxyHelper.class);
		
        // Start the log file
		bind(LogConfiguration.class).toProvider(LogConfigurationProvider.class).in(Scopes.SINGLETON);		
		bind(LogListener.class).toProvider(FileLogListenerProvider.class).in(Scopes.SINGLETON);  
		
		// this needs to be removed.
		binder().requestStaticInjection(LogManager.class);
		
	}
	
	int getUnicastClusterPort() {
		try {
			com.metamatrix.common.config.api.Configuration config = CurrentConfiguration.getInstance().getConfiguration();
			VMComponentDefn process = config.getVMForHost(this.host.getFullName(), this.processName);
			String strPort = process.getProperties().getProperty(VMComponentDefnType.CLUSTER_PORT);
			if (strPort != null && strPort.length() > 0) {
				return Integer.parseInt(strPort);
			}
		} catch (ConfigurationException e) {
			throw new MetaMatrixRuntimeException(e);
		} catch (NumberFormatException e) {
			throw new MetaMatrixRuntimeException(e);
		}
		return 5555;
	}
}
