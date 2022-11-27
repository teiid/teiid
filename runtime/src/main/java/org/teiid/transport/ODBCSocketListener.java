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
package org.teiid.transport;

import java.net.InetSocketAddress;

import org.teiid.common.buffer.StorageManager;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.net.socket.ObjectChannel;

import io.netty.channel.ChannelPipeline;

public class ODBCSocketListener extends SocketListener {
    private int maxBufferSize = PropertiesUtils.getHierarchicalProperty("org.teiid.ODBCPacketSize", 307200, Integer.class); //$NON-NLS-1$
    private boolean requireSecure = PropertiesUtils.getHierarchicalProperty("org.teiid.ODBCRequireSecure", true, Boolean.class); //$NON-NLS-1$
    private int maxLobSize;
    private TeiidDriver driver;
    private LogonImpl logonService;

    public ODBCSocketListener(InetSocketAddress address, SocketConfiguration config, final ClientServiceRegistryImpl csr, StorageManager storageManager, int maxLobSize, LogonImpl logon, TeiidDriver driver) {
        //the clientserviceregistry isn't actually used by ODBC
        super(address, config, csr, storageManager);
        this.maxLobSize = maxLobSize;
        this.driver = driver;
        this.logonService = logon;
    }

    public void setDriver(TeiidDriver driver) {
        this.driver = driver;
    }

    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    protected void configureChannelPipeline(ChannelPipeline pipeline,
            SSLConfiguration config, StorageManager storageManager) throws Exception {
        PgBackendProtocol pgBackendProtocol = new PgBackendProtocol(maxLobSize, maxBufferSize, config, requireSecure);
        pipeline.addLast("odbcFrontendProtocol", new PgFrontendProtocol(pgBackendProtocol, 1 << 20)); //$NON-NLS-1$
        pipeline.addLast("odbcBackendProtocol", pgBackendProtocol); //$NON-NLS-1$
        pipeline.addLast("handler", this.channelHandler); //$NON-NLS-1$
    }

    @Override
    public ChannelListener createChannelListener(ObjectChannel channel) {
        return new ODBCClientInstance(channel, driver, logonService);
    }

    public void setRequireSecure(boolean requireSecure) {
        this.requireSecure = requireSecure;
    }
}
