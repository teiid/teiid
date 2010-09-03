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
package org.teiid.cache.jboss;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.cache.CacheStatus;
import org.jboss.cache.Fqn;
import org.jboss.cache.Modification;
import org.jboss.cache.RegionManager;
import org.jboss.cache.ReplicationException;
import org.jboss.cache.commands.CommandsFactory;
import org.jboss.cache.commands.DataCommand;
import org.jboss.cache.commands.read.ExistsCommand;
import org.jboss.cache.commands.read.GetDataMapCommand;
import org.jboss.cache.commands.remote.ClusteredGetCommand;
import org.jboss.cache.config.CacheLoaderConfig.IndividualCacheLoaderConfig;
import org.jboss.cache.factories.annotations.Inject;
import org.jboss.cache.loader.AbstractCacheLoader;
import org.jboss.cache.lock.StripedLock;
import org.jgroups.Address;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.RspFilter;

class ClusteredTupleBatchCacheLoader extends AbstractCacheLoader {
	private StripedLock lock = new StripedLock();
	private TupleBatchCacheLoaderConfig config;
	private CommandsFactory commandsFactory;

	private boolean init = false;

	@Override
	public void start() {
		init();
	}

	private void init() {
		if (!this.init) {
			setCommandsFactory(cache.getComponentRegistry().getComponent(CommandsFactory.class));
			this.init = true;
		}
	}

	/**
	 * A test to check whether the cache is in its started state. If not, calls
	 * should not be made as the channel may not have properly started, blocks
	 * due to state transfers may be in progress, etc.
	 * 
	 * @return true if the cache is in its STARTED state.
	 */
	protected boolean isCacheReady() {
		return cache.getCacheStatus() == CacheStatus.STARTED;
	}

	@Inject
	public void setCommandsFactory(CommandsFactory commandsFactory) {
		this.commandsFactory = commandsFactory;
	}

	/**
	 * Sets the configuration. A property <code>timeout</code> is used as the
	 * timeout value.
	 */
	public void setConfig(IndividualCacheLoaderConfig base) {
		this.config = (TupleBatchCacheLoaderConfig) base;
	}

	public IndividualCacheLoaderConfig getConfig() {
		return config;
	}

	public Set getChildrenNames(Fqn fqn) throws Exception {
		return Collections.emptySet();
	}

	private List<Object> callRemote(DataCommand dataCommand) throws Exception {
		ClusteredGetCommand clusteredGet = commandsFactory	.buildClusteredGetCommand(false, dataCommand);
		List resps;
		// JBCACHE-1186
		resps = cache.getRPCManager().callRemoteMethods(null,	clusteredGet, GroupRequest.GET_ALL, config.getTimeout(), new ResponseValidityFilter(cache.getMembers(), cache.getLocalAddress(), dataCommand), false);

		if (resps == null) {
			throw new ReplicationException("No replies to call " + dataCommand	+ ".  Perhaps we're alone in the cluster?"); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		return resps;
	}

	public Map get(Fqn name) throws Exception {
		return get0(name);
	}

	protected Map get0(Fqn name) throws Exception {
		// DON'T make a remote call if this is a remote call in the first place
		// - leads to deadlocks - JBCACHE-1103
		if (!isCacheReady() || !cache.getInvocationContext().isOriginLocal()) {
			return null;
		}
		// return Collections.emptyMap();
		lock.acquireLock(name, true);
		try {
			init();
			GetDataMapCommand command = commandsFactory.buildGetDataMapCommand(name);
			List resps = callRemote(command);
			
			Map result = Collections.EMPTY_MAP;
			for (Object o:resps) {
				if (o != null && !(o instanceof Exception)) {
					List clusteredGetResp = (List) o;
					if ((Boolean)clusteredGetResp.get(0)) {
						Map resp = (Map)clusteredGetResp.get(1);
						if (!resp.isEmpty()) {
							result = resp;
						}
					}
				}
			}			
			return result;
		} finally {
			lock.releaseLock(name);
		}
	}

	public boolean exists(Fqn name) throws Exception {
		// DON'T make a remote call if this is a remote call in the first place
		// - leads to deadlocks - JBCACHE-1103
		if (!isCacheReady() || !cache.getInvocationContext().isOriginLocal()) {
			return false;
		}

		lock.acquireLock(name, false);
		try {
			init();
			ExistsCommand command = commandsFactory.buildExistsNodeCommand(name);
			List resps = callRemote(command);
			boolean result = false;
			for (Object o:resps) {
				if (o != null && !(o instanceof Exception)) {
					List<Boolean> clusteredGetResp = (List<Boolean>) o;
					if (clusteredGetResp.get(0)) {
						if (clusteredGetResp.get(1)) {
							result = true;
							break;
						}
					}
				}
			}
			return result;
		} finally {
			lock.releaseLock(name);
		}
	}

	public Object put(Fqn name, Object key, Object value) throws Exception {
		return null;
	}

	/**
	 * Does nothing; replication handles put.
	 */
	public void put(Fqn name, Map attributes) throws Exception {
	}

	/**
	 * Does nothing; replication handles put.
	 */
	@Override
	public void put(List<Modification> modifications) throws Exception {
	}

	/**
	 * Fetches the remove value, does not remove. Replication handles removal.
	 */
	public Object remove(Fqn name, Object key) throws Exception {
		return null;
	}

	/**
	 * Does nothing; replication handles removal.
	 */
	public void remove(Fqn name) throws Exception {
		// do nothing
	}

	/**
	 * Does nothing; replication handles removal.
	 */
	public void removeData(Fqn name) throws Exception {
	}

	/**
	 * Does nothing.
	 */
	@Override
	public void prepare(Object tx, List modifications, boolean one_phase)
			throws Exception {
	}

	/**
	 * Does nothing.
	 */
	@Override
	public void commit(Object tx) throws Exception {
	}

	/**
	 * Does nothing.
	 */
	@Override
	public void rollback(Object tx) {
	}

	@Override
	public void loadEntireState(ObjectOutputStream os) throws Exception {
		// intentional no-op
	}

	@Override
	public void loadState(Fqn subtree, ObjectOutputStream os) throws Exception {
		// intentional no-op
	}

	@Override
	public void storeEntireState(ObjectInputStream is) throws Exception {
		// intentional no-op
	}

	@Override
	public void storeState(Fqn subtree, ObjectInputStream is) throws Exception {
		// intentional no-op
	}

	@Override
	public void setRegionManager(RegionManager manager) {
	}

	public static class ResponseValidityFilter implements RspFilter {
		private int numValidResponses = 0;
		private List<Address> pendingResponders;
		private DataCommand command;

		public ResponseValidityFilter(List<Address> expected,Address localAddress, DataCommand command) {
			this.pendingResponders = new ArrayList<Address>(expected);
			// We'll never get a response from ourself
			this.pendingResponders.remove(localAddress);
			this.command = command;
		}

		public boolean isAcceptable(Object object, Address address) {
			pendingResponders.remove(address);

			if (object instanceof List) {
				List response = (List) object;
				Boolean foundResult = (Boolean) response.get(0);
				if (foundResult) {
					if (command instanceof ExistsCommand) {
						Boolean resp = (Boolean)response.get(1);
						if (resp) {
							numValidResponses++;		
						}
					}
					else if (command instanceof GetDataMapCommand) {
						Map resp = (Map)response.get(1);
						if (!resp.isEmpty()) {
							numValidResponses++;
						}
					}
				}
			}
			// always return true to make sure a response is logged by the
			// JGroups RpcDispatcher.
			return true;
		}

		public boolean needMoreResponses() {
			return numValidResponses < 1 && pendingResponders.size() > 0;
		}

	}
}
