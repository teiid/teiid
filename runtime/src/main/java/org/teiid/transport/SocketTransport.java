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

import org.teiid.runtime.RuntimePlugin;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;

/**
 * This class starts a Socket for DQP connections and listens on the port and hands out the connections to the 
 * users 
 */
public class SocketTransport {
        
	private SocketListener listener;
	private SocketConfiguration config;
	private ClientServiceRegistryImpl csr;
	
	public SocketTransport(SocketConfiguration config, ClientServiceRegistryImpl csr) {
		this.config = config;
		this.csr = csr;
	}
	
    public void start() {
        String bindAddress = this.config.getHostAddress().getHostAddress();
        
		LogManager.logDetail(LogConstants.CTX_SERVER, RuntimePlugin.Util.getString("SocketTransport.1", new Object[] {bindAddress, String.valueOf(this.config.getPortNumber())})); //$NON-NLS-1$
		this.listener = new SocketListener(this.config.getPortNumber(), bindAddress, this.config.getInputBufferSize(), this.config.getOutputBufferSize(), this.config.getMaxSocketThreads(), this.config.getSSLConfiguration(), csr);
    }
    
    public void stop() {
    	this.listener.stop();
    }
    
    public SocketListenerStats getStats() {
    	return this.listener.getStats();
    }    
 	
}
