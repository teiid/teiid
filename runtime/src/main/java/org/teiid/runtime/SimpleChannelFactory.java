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

import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.fork.ForkChannel;
import org.teiid.replication.jgroups.ChannelFactory;

final class SimpleChannelFactory implements ChannelFactory {
    private final EmbeddedConfiguration embeddedConfiguration;
    private JChannel channel;

    /**
     * @param embeddedConfiguration
     */
    SimpleChannelFactory(EmbeddedConfiguration embeddedConfiguration) {
        this.embeddedConfiguration = embeddedConfiguration;
    }

    @Override
    public Channel createChannel(String id) throws Exception {
    	synchronized (this) {
        	if (channel == null) {
        		channel = new JChannel(this.getClass().getClassLoader().getResource(this.embeddedConfiguration.getJgroupsConfigFile()));
        		channel.connect("teiid-replicator"); //$NON-NLS-1$
        	}
		}
    	//assumes fork and other necessary protocols are in the main stack
    	ForkChannel fc = new ForkChannel(channel, "teiid-replicator-fork", id); //$NON-NLS-1$
        return fc;
    }

    void stop() {
    	if (this.channel != null) {
    		channel.close();
    	}
    }
}