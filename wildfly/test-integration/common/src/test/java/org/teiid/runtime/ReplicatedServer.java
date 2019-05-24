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

package org.teiid.runtime;

import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.transaction.tm.DummyTransactionManager;
import org.teiid.jboss.NodeTracker;
import org.teiid.jdbc.FakeServer;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.ObjectReplicator;
import org.teiid.replication.jgroups.JGroupsObjectReplicator;

@SuppressWarnings("nls")
public class ReplicatedServer extends FakeServer {

    private SimpleChannelFactory channelFactory;
    private NodeTracker nodeTracker;

    static {
        System.setProperty("jgroups.bind_addr", "127.0.0.1");
    }

    public ReplicatedServer(boolean start) {
        super(start);
    }

    public static ReplicatedServer createServer(String nodeName, String ispn, String jgroups) throws Exception {
        ReplicatedServer server = new ReplicatedServer(false);
        server.channelFactory = new SimpleChannelFactory(jgroups);

        EmbeddedConfiguration config = new EmbeddedConfiguration() {
            @Override
            public ObjectReplicator getObjectReplicator() {
                return new JGroupsObjectReplicator(server.channelFactory, server.scheduler);
            }
        };
        config.setInfinispanConfigFile(ispn);
        config.setTransactionManager(new DummyTransactionManager());
        config.setNodeName(nodeName);
        server.start(config, true);

        return server;
    }

    @Override
    public synchronized void start(EmbeddedConfiguration config) {
        super.start(config);
        try {
            this.nodeTracker = new NodeTracker(channelFactory.createChannel("teiid-node-tracker"), config.getNodeName()) {
                @Override
                public ScheduledExecutorService getScheduledExecutorService() {
                    return scheduler;
                }
            };
            this.nodeTracker.addNodeListener(this.materializationMgr);
        } catch (Exception e) {
            LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40089));
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
        this.channelFactory.stop();
    }

    public void setObjectReplicator(ObjectReplicator replicator) {
        this.replicator = replicator;
    }

    public ObjectReplicator getObjectReplicator() {
        return this.replicator;
    }

}
