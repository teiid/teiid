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

import com.metamatrix.common.util.NetUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;

public class SocketConfiguration {
	private int outputBufferSize;
	private int inputBufferSize;
	private int maxSocketThreads;
	private int portNumber;
	private InetAddress hostAddress;
	private SSLConfiguration sslConfiguration;
	private boolean enabled;
	
	
	public void setBindAddress(String addr) {
		this.hostAddress = resolveHostAddress(addr);
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
 	
 	private InetAddress resolveHostAddress(String bindAddress) {
		try {
			if (bindAddress == null) {
				return NetUtils.getInstance().getInetAddress();
			}
			return NetUtils.resolveHostByName(bindAddress);	
		} catch (UnknownHostException e) {
			throw new MetaMatrixRuntimeException("Failed to resolve the bind address"); //$NON-NLS-1$
		}
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
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
		return hostAddress;
	}

	public SSLConfiguration getSSLConfiguration() {
		return sslConfiguration;
	}	 	
}
