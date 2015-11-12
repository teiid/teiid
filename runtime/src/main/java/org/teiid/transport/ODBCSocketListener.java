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
package org.teiid.transport;

import io.netty.channel.ChannelPipeline;

import java.net.InetSocketAddress;

import org.teiid.common.buffer.StorageManager;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.net.socket.ObjectChannel;

public class ODBCSocketListener extends SocketListener {
	private int maxBufferSize = PropertiesUtils.getIntProperty(System.getProperties(), "org.teiid.ODBCPacketSize", 307200); //$NON-NLS-1$
	private boolean requireSecure = PropertiesUtils.getBooleanProperty(System.getProperties(), "org.teiid.ODBCRequireSecure", true); //$NON-NLS-1$
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
