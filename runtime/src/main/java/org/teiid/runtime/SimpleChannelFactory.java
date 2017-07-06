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