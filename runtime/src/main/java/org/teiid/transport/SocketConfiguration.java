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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.runtime.RuntimePlugin;


public class SocketConfiguration {
	
	private int outputBufferSize;
	private int inputBufferSize;
	private int maxSocketThreads;
	private int portNumber;
	private InetAddress hostAddress;
	private SSLConfiguration sslConfiguration;
	private String hostName;
	private String name;
	private WireProtocol protocol = WireProtocol.teiid;
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setBindAddress(String addr) {
		this.hostName = addr;
	}
	
	public void setPortNumber(int port) {
		this.portNumber = port;
	}
	
	public void setMaxSocketThreads(int value) {
		this.maxSocketThreads = value;
	}
	
	public void setInputBufferSize(int value) {
		this.inputBufferSize = value;
	}
	
	public void setOutputBufferSize(int value) {
		this.outputBufferSize = value;
	}
	
	public void setSSLConfiguration(SSLConfiguration value) {
		this.sslConfiguration = value;
	}	
 	
	private void resolveHostName() {
		try {
			// if not defined then see if can bind to local address; if supplied resolve it by name
			if (this.hostName == null) {
				this.hostName = InetAddress.getLocalHost().getHostName();
			}
		} catch (UnknownHostException e) {
			 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40065, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40065));
		}
	}

	public int getOutputBufferSize() {
		return outputBufferSize;
	}

	public int getInputBufferSize() {
		return inputBufferSize;
	}

	public int getMaxSocketThreads() {
		return maxSocketThreads;
	}

	public int getPortNumber() {
		return portNumber;
	}

	public InetAddress getHostAddress() {
		resolveHostName();
		
		if (this.hostAddress != null) {
			return hostAddress;
		}
    	try {
    		//only cache inetaddresses if they represent the ip. 
			InetAddress addr = InetAddress.getByName(this.hostName);
			if (addr.getHostAddress().equalsIgnoreCase(this.hostName)) {
				this.hostAddress = addr;
			}
			return addr;
		} catch (UnknownHostException e) {
			 throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40065, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40065));
		}		
	}
	
	public void setHostAddress(InetAddress hostAddress) {
		this.hostAddress = hostAddress;
		this.hostName = hostAddress.getHostName();
	}	
	
	public String getHostName() {
		resolveHostName();
		return this.hostName;
	}

	public SSLConfiguration getSSLConfiguration() {
		return sslConfiguration;
	}
	
	public boolean getSslEnabled() {
		return this.sslConfiguration != null && this.sslConfiguration.isSslEnabled();
	}

	public WireProtocol getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = WireProtocol.valueOf(protocol);
	}
	
	public void setProtocol(WireProtocol protocol) {
		this.protocol = protocol;
	}	
}
