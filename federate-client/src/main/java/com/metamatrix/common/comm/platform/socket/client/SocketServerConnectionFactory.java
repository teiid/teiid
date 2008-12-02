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
import java.util.Properties;
import java.util.Timer;

import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.common.api.MMURL_Properties.CONNECTION;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.socket.SocketConstants;
import com.metamatrix.common.comm.platform.socket.SocketLog;
import com.metamatrix.common.util.MetaMatrixProductNames;
import com.metamatrix.common.util.NetUtils;

public class SocketServerConnectionFactory implements ServerConnectionFactory, SocketServerInstanceFactory {
	
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
	
	public SocketServerInstance createServerInstance(HostInfo info, boolean ssl) throws CommunicationException, IOException {
		return new SocketServerInstanceImpl(info, ssl, this.log, this.channelFactory);
	}
	
	/**
	 * @param connectionProperties will be updated with additional information before logon
	 */
	public SocketServerConnection createConnection(Properties connectionProperties) throws CommunicationException, ConnectionException {
		
		updateConnectionProperties(connectionProperties);
		
		MMURL url = new MMURL(connectionProperties.getProperty(MMURL_Properties.SERVER.SERVER_URL));
		
		return new SocketServerConnection(this, url.isUsingSSL(), new UrlServerDiscovery(url), connectionProperties, pingTimer);
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
