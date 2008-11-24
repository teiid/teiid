/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.comm.platform.socket.client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;

import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.common.api.MMURL_Properties.CONNECTION;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.comm.platform.socket.SocketConstants;
import com.metamatrix.common.comm.platform.socket.SocketLog;
import com.metamatrix.common.util.MetaMatrixProductNames;
import com.metamatrix.common.util.NetUtils;

public class SocketServerConnectionFactory implements ServerConnectionFactory {
	
	private static SocketServerConnectionFactory INSTANCE;
	
    private SocketLog log = SocketConstants.getLog(SocketServerConnectionFactory.class.getSimpleName());
    private ObjectChannelFactory channelFactory = new NioObjectChannelFactory(
			SocketConstants.getConserveBandwidth(), SocketConstants
					.getInputBufferSize(), SocketConstants
					.getOutputBufferSize(), Thread.currentThread()
					.getContextClassLoader());
	private Timer pingTimer = new Timer("SocketPing", true); //$NON-NLS-1$
	
	public static synchronized SocketServerConnectionFactory getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new SocketServerConnectionFactory();
		}
		return INSTANCE;
	}
	
	SocketServerInstanceImpl establishConnection(MMURL url) throws CommunicationException {
		
		List<HostInfo> hostKeys = new ArrayList<HostInfo>(url.getHostInfo());
		
		for (int i = 0; i < hostKeys.size(); i++) {
			HostInfo hostInfo = hostKeys.remove((int)(Math.random() * hostKeys.size()));
			
			SocketServerInstanceImpl serverInstance = null;
			try {
				serverInstance = new SocketServerInstanceImpl(hostInfo, url.isUsingSSL(), this.log, this.channelFactory);
			} catch (IOException e) {
				if (url.getHostInfo().size() == 1) {
					throw hostException(hostInfo, e);
				}
				continue;
			}
			
	        return serverInstance;
		}
        throw new CommunicationException(CommPlatformPlugin.Util.getString("SocketServerInstancePool.No_valid_host_available"))  ; //$NON-NLS-1$
    }

	private CommunicationException hostException(HostInfo hostInfo, IOException e) {
		if (e instanceof UnknownHostException) {
			return new CommunicationException(e, CommPlatformPlugin.Util.getString("SocketServerInstance.Connection_Error.Uknown_Host", hostInfo.getHostName() ) ); //$NON-NLS-1$
		}
		return new CommunicationException(e, CommPlatformPlugin.Util.getString("SocketServerInstance.Connection_Error.Connect_Failed", hostInfo.getHostName(), String.valueOf(hostInfo.getPortNumber()), e.getMessage() )); //$NON-NLS-1$
	}
	
	/**
	 * @param connectionProperties will be updated with additional information before logon
	 */
	public ServerConnection createConnection(Properties connectionProperties) throws CommunicationException, ConnectionException {
		
		updateConnectionProperties(connectionProperties);
		
		MMURL url = new MMURL(connectionProperties.getProperty(MMURL_Properties.SERVER.SERVER_URL));
		
		SocketServerInstanceImpl connection = establishConnection(url);
		
		return new SocketServerConnection(connection, connectionProperties, pingTimer);
	}

	static void updateConnectionProperties(Properties connectionProperties) {
		try {
			connectionProperties.put(CONNECTION.CLIENT_IP_ADDRESS, NetUtils.getHostAddress());
        } catch (UnknownHostException err1) {
        	connectionProperties.put(CONNECTION.CLIENT_IP_ADDRESS, "UnknownClientAddress"); //$NON-NLS-1$
        }
        
        try {
        	connectionProperties.put(CONNECTION.CLIENT_HOSTNAME, NetUtils.getHostname());
        } catch (UnknownHostException err1) {
        	connectionProperties.put(CONNECTION.CLIENT_HOSTNAME, "UnknownClientHost"); //$NON-NLS-1$
        }
               
		String productName = connectionProperties.getProperty(MMURL_Properties.CONNECTION.PRODUCT_NAME);
		
		if (MetaMatrixProductNames.Platform.PRODUCT_NAME.equalsIgnoreCase(productName)) {
			connectionProperties.setProperty(MMURL_Properties.CONNECTION.AUTO_FAILOVER, Boolean.TRUE.toString());
		}
	}

}
