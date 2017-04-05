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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;

public abstract class NodeTracker extends ReceiverAdapter{
    
    public interface NodeListener {
        void nodeJoined(String nodeName);
        void nodeDropped(String nodeName);
    }
    public abstract ScheduledExecutorService getScheduledExecutorService();    
    private Map<Address, String> nodes = new HashMap<>();
    private Channel channel;
    private String nodeName;
    private Set<NodeListener> nodeListeners = Collections.synchronizedSet(new HashSet<NodeListener>());
    
    public NodeTracker(Channel channel, String nodeName) throws Exception {
        this.nodeName = nodeName;
        this.channel = channel;
        this.channel.setReceiver(this);
        this.channel.connect("teiid-node-tracker");
    }
    
    public void addNodeListener(NodeListener nodeListener) {        
        this.nodeListeners.add(nodeListener);
    }
    
    public void removeNodeListener(NodeListener nodeListener) {        
        this.nodeListeners.remove(nodeListener);
    }
    
    public boolean isNodeAlive(String nodeName) {
        if (this.nodeName.equalsIgnoreCase(nodeName)) {
            return true;
        }        
        return nodes.values().contains(nodeName);
    }
    
    @Override
    public void viewAccepted(View view) {
        Map<Address, String> newMembers = new HashMap<>();
        Map<Address, String> deadMembers = null;
        if (view.getMembers() != null && !this.nodes.isEmpty()) {
            synchronized (nodes) {
                for (Address addr : view.getMembers()) {
                    String name = this.nodes.remove(addr);
                    if (name != null) {
                        // existing node
                        newMembers.put(addr, name);
                    }
                }
                deadMembers = this.nodes;
                this.nodes = newMembers;
            }
            if (!deadMembers.isEmpty()) {
                for (String name : deadMembers.values()) {
                    // node removed
                    if (!this.nodeListeners.isEmpty()) {
                        for (NodeListener nl : this.nodeListeners) {
                            nl.nodeDropped(name);
                        }
                    }
                }
            }
        }      
        
        getScheduledExecutorService().schedule(
        new Runnable() {
            public void run() {
                Message msg=new Message(null, null, nodeName);
                try {
                    channel.send(msg);
                } catch (Exception e) {
                    LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40165));
                }        
            }
        }, 2000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void receive(Message msg) {
        synchronized (this.nodes) {
            String prevNode = this.nodes.put(msg.getSrc(), (String)msg.getObject());    
            // node added
            if (prevNode == null) {
                for (NodeListener nl : this.nodeListeners) {
                    nl.nodeJoined((String)msg.getObject());
                }
            }
        }
    }    
}