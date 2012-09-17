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
package org.teiid.jboss;

import java.util.concurrent.Executor;

import org.jboss.as.clustering.jgroups.ChannelFactory;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.jgroups.Channel;
import org.teiid.replication.jgroups.JGroupsObjectReplicator;

class JGroupsObjectReplicatorService implements Service<JGroupsObjectReplicator> {

	public final InjectedValue<ChannelFactory> channelFactoryInjector = new InjectedValue<ChannelFactory>();
	final InjectedValue<Executor> executorInjector = new InjectedValue<Executor>();
	private JGroupsObjectReplicator replicator; 
	
	
	@Override
	public void start(StartContext context) throws StartException {
		this.replicator = new JGroupsObjectReplicator(new org.teiid.replication.jgroups.ChannelFactory() {
			@Override
			public Channel createChannel(String id) throws Exception {
				return channelFactoryInjector.getValue().createChannel(id);
			}
		}, executorInjector.getValue());
	}

	@Override
	public void stop(StopContext context) {
	}

	@Override
	public JGroupsObjectReplicator getValue() throws IllegalStateException,IllegalArgumentException {
		return replicator;
	}
	
}
