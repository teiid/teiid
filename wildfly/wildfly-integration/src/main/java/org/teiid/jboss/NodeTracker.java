/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.jboss;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.NodeListener;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.runtime.RuntimePlugin.Event;

public abstract class NodeTracker extends ReceiverAdapter{

    public abstract ScheduledExecutorService getScheduledExecutorService();
    private Map<Address, String> nodes = new HashMap<>();
    private JChannel channel;
    private String nodeName;
    private Set<NodeListener> nodeListeners = Collections.synchronizedSet(new HashSet<NodeListener>());

    public NodeTracker(JChannel channel, String nodeName) throws Exception {
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
                Message msg=new Message(null, nodeName);
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