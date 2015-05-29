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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import org.jgroups.Channel;
import org.jgroups.ChannelListener;
import org.jgroups.JChannel;
import org.teiid.replication.jgroups.ChannelFactory;

final class SimpleChannelFactory implements ChannelFactory, ChannelListener {
    private final EmbeddedConfiguration embeddedConfiguration;

    /**
     * @param embeddedConfiguration
     */
    SimpleChannelFactory(EmbeddedConfiguration embeddedConfiguration) {
        this.embeddedConfiguration = embeddedConfiguration;
    }

    private final Map<Channel, String> channels = Collections.synchronizedMap(new WeakHashMap<Channel, String>());
    
    @Override
    public Channel createChannel(String id) throws Exception {
        JChannel channel = new JChannel(this.getClass().getClassLoader().getResource(this.embeddedConfiguration.getJgroupsConfigFile()));
        channels.put(channel, id);
        channel.addChannelListener(this);
        return channel;
    }

    @Override
    public void channelClosed(Channel channel) {
        channels.remove(channel);
    }

    @Override
    public void channelConnected(Channel channel) {
    }

    @Override
    public void channelDisconnected(Channel channel) {
    }
    
    void stop() {
        synchronized (channels) {
            for (Channel c : new ArrayList<Channel>(channels.keySet())) {
                c.close();
            }
            channels.clear();
        }
    }
}