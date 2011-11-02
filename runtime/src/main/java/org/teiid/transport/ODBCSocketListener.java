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

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.teiid.client.security.ILogon;
import org.teiid.common.buffer.StorageManager;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.net.TeiidURL.CONNECTION.AuthenticationType;
import org.teiid.net.socket.ObjectChannel;

public class ODBCSocketListener extends SocketListener {
	private AuthenticationType authType = AuthenticationType.CLEARTEXT;
	private int maxBufferSize = Integer.parseInt(System.getProperty("org.teiid.ODBCPacketSize", "307200")); //$NON-NLS-1$ //$NON-NLS-2$
	private int maxLobSize;
	private TeiidDriver driver;
	private ILogon logonService;
	
	public ODBCSocketListener(SocketConfiguration config, StorageManager storageManager, int portOffset, int maxLobSize, ILogon logon) {
		//the clientserviceregistry isn't actually used by ODBC 
		super(config, new ClientServiceRegistryImpl(ClientServiceRegistry.Type.ODBC), storageManager, portOffset);
		this.maxLobSize = maxLobSize;
		this.driver = new TeiidDriver();
		this.logonService = logon;
	}
	
	public void setDriver(TeiidDriver driver) {
		this.driver = driver;
	}
	
	public void setMaxBufferSize(int maxBufferSize) {
		this.maxBufferSize = maxBufferSize;
	}

	@Override
	protected SSLAwareChannelHandler createChannelPipelineFactory(final SSLConfiguration config, final StorageManager storageManager) {
		return new SSLAwareChannelHandler(this, config, Thread.currentThread().getContextClassLoader(), storageManager) {
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = new DefaultChannelPipeline();

			    pipeline.addLast("odbcFrontendProtocol", new PgFrontendProtocol(1 << 20)); //$NON-NLS-1$
			    pipeline.addLast("odbcBackendProtocol", new PgBackendProtocol(maxLobSize, maxBufferSize, config)); //$NON-NLS-1$
			    pipeline.addLast("handler", this); //$NON-NLS-1$
			    return pipeline;
			}			
		};
	}
	
	@Override
	public ChannelListener createChannelListener(ObjectChannel channel) {
		return new ODBCClientInstance(channel, this.authType, driver, logonService);
	}

	public void setAuthenticationType(AuthenticationType value) {
		this.authType = value;
	}

}
