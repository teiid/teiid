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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.Properties;

import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.vm.controller.SocketListenerStats;

/**
 * This class starts a Socket for DQP connections and listens on the port and hands out the connections to the 
 * users 
 */
public class SocketTransport {
        
    private static final int DEFAULT_SERVER_PORT = 31000;
    private static final int DEFAULT_MAX_THREADS = 15;
    private static final int DEFAULT_INPUT_BUFFER_SIZE = 0;
    private static final int DEFAULT_OUTPUT_BUFFER_SIZE = 0;
	
	private Properties props; 
	private SocketListener listener;
	
	private SessionServiceInterface sessionSerice;
	protected ClientServiceRegistry clientServices;
	
	public SocketTransport(Properties props, ClientServiceRegistry serivices, SessionServiceInterface sessionService) {
		this.props = props;
		this.clientServices = serivices;
		this.sessionSerice = sessionService;
	}
	
    public void start() {
        int socketPort = PropertiesUtils.getIntProperty(this.props, DQPEmbeddedProperties.SERVER_PORT, DEFAULT_SERVER_PORT);
        int maxThreads = PropertiesUtils.getIntProperty(this.props, DQPEmbeddedProperties.MAX_THREADS, DEFAULT_MAX_THREADS);
        int inputBufferSize = PropertiesUtils.getIntProperty(this.props, DQPEmbeddedProperties.INPUT_BUFFER_SIZE, DEFAULT_INPUT_BUFFER_SIZE);
        int outputBufferSize = PropertiesUtils.getIntProperty(this.props, DQPEmbeddedProperties.OUTPUT_BUFFER_SIZE, DEFAULT_OUTPUT_BUFFER_SIZE);
        String bindAddress = ((InetAddress)props.get(DQPEmbeddedProperties.HOST_ADDRESS)).getHostAddress();
        
        try {
			SSLConfiguration helper = new SSLConfiguration();
			helper.init(this.props);
			
			LogManager.logDetail(LogConstants.CTX_SERVER, DQPEmbeddedPlugin.Util.getString("SocketTransport.1", new Object[] {bindAddress, String.valueOf(socketPort)})); //$NON-NLS-1$
			this.listener = new SocketListener(socketPort, bindAddress, this.clientServices, inputBufferSize, outputBufferSize, maxThreads, helper.getServerSSLEngine(), helper.isClientEncryptionEnabled(), this.sessionSerice);
			
		} catch (UnknownHostException e) {
			throw new MetaMatrixRuntimeException(e, DQPEmbeddedPlugin.Util.getString("SocketTransport.2",new Object[] {bindAddress, String.valueOf(socketPort)})); //$NON-NLS-1$
		} catch (IOException e) {
			throw new MetaMatrixRuntimeException(e, DQPEmbeddedPlugin.Util.getString("SocketTransport.2",new Object[] {bindAddress, String.valueOf(socketPort)})); //$NON-NLS-1$
		} catch (GeneralSecurityException e) {
			throw new MetaMatrixRuntimeException(e, DQPEmbeddedPlugin.Util.getString("SocketTransport.2",new Object[] {bindAddress, String.valueOf(socketPort)})); //$NON-NLS-1$
		}        
    }
    
    public void stop() {
    	this.listener.stop();
    }
    
    public WorkerPoolStats getProcessPoolStats() {
    	return listener.getProcessPoolStats();
    }
    
    public int getPort() {
    	return this.listener.getPort();
    }
       
    public SocketListenerStats getStats() {
    	return this.listener.getStats();
    }    
}
