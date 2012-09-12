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
package org.teiid.translator.object.infinispan;

import org.infinispan.api.BasicCacheContainer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.CacheContainer;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.Translator;

/**
 * The InfinispanRemoteExecutionFactory is used to obtain a remote
 * {@link CacheContainer}. The {@link #getRemoteServerList() serverList} must
 * provide 1 or more <code>host:port[;host:port...]</code> combinations that
 * indicate where the remote server(s) are located.
 * 
 * @author vhalbert
 * 
 */

@Translator(name = "infinispanRemoteCache", description = "The Execution Factory for Remote Infinispan Cache")
public class InfinispanRemoteExecutionFactory extends
		InfinispanExecutionFactory {
	// public static final String DATAGRID_HOST = "datagrid.host";
	// public static final String HOTROD_PORT = "datagrid.hotrod.port";

	private BasicCacheContainer manager;

	private volatile String remoteServerList;

	public InfinispanRemoteExecutionFactory() {
		super();
		this.setSourceRequired(false);
	}


	protected boolean createCacheContainer() {
		if (this.getConfigurationFileName() != null) {
			return true;
		}

		if (this.getRemoteServerList() != null
				|| this.getRemoteServerList().length() > 0) {
			return true;
		}

		return false;

	}

	@Override
	public boolean isSourceRequired() {
		return true;
	}

	/**
	 * Get the list of remote servers that make up the Infinispan cluster. The
	 * servers must be Infinispan HotRod servers. The list must be in the
	 * appropriate format of <code>host:port[;host:port...]</code> that would be
	 * used when defining an Infinispan {@link RemoteCacheManager} instance. If
	 * the value is missing, <code>localhost:11311</code> is assumed.
	 * 
	 * @return the names of the remote servers
	 */
	public String getRemoteServerList() {
		return remoteServerList;
	}

	/**
	 * Set the list of remote servers that make up the Infinispan cluster. The
	 * servers must be Infinispan HotRod servers. The list must be in the
	 * appropriate format of <code>host:port[;host:port...]</code> that would be
	 * used when defining an Infinispan {@link RemoteCacheManager} instance. If
	 * the value is missing, <code>localhost:11311</code> is assumed.
	 * 
	 * @param remoteInfinispanServerList
	 *            the server list in appropriate
	 *            <code>server:port;server2:port2</code> format.
	 * 
	 * @see #getRemoteServerList()
	 */
	public synchronized void setRemoteServerList(
			String remoteInfinispanServerList) {
		if (this.remoteServerList == remoteInfinispanServerList
				|| this.remoteServerList != null
				&& this.remoteServerList.equals(remoteInfinispanServerList))
			return; // unchanged
		this.remoteServerList = remoteInfinispanServerList;
	}

	@Override
	protected synchronized BasicCacheContainer getCacheContainer() {
		RemoteCacheManager container = null;
		if (this.getConfigurationFileName() != null) {
			container = new RemoteCacheManager(this.getConfigurationFileName());
			
			LogManager
			.logInfo(LogConstants.CTX_CONNECTOR,
					"=== Using RemoteCacheManager (loaded by configuration) ==="); //$NON-NLS-1$

		} else {
			if (this.getRemoteServerList() == null
					|| this.getRemoteServerList().isEmpty()
					|| this.getRemoteServerList().equals("")) {
				container = new RemoteCacheManager();
				
				LogManager
				.logInfo(LogConstants.CTX_CONNECTOR,
						"=== Using RemoteCacheManager (no serverlist defined) ==="); //$NON-NLS-1$

			} else {
				container = new RemoteCacheManager(this.getRemoteServerList());
				
				LogManager
				.logInfo(LogConstants.CTX_CONNECTOR,
						"=== Using RemoteCacheManager (loaded by serverlist) ==="); //$NON-NLS-1$

			}
		}

		return container;

	}

	public void cleanUp() {
		manager.stop();
		manager = null;
	}

}
