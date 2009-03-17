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

package com.metamatrix.common.api;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.metamatrix.common.util.NetUtils;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.HashCodeUtil;

/**
 * Defines the hostname/port or {@link InetAddress} to connect to a host.
 * @since 4.2
 */
public class HostInfo {
    // Host Name and Port Number
    private String hostName;
    private int portNumber = 0;
    private InetAddress inetAddress;

    public InetAddress getInetAddress() throws UnknownHostException {
    	if (inetAddress != null) {
    		return inetAddress;
    	}
    	return NetUtils.resolveHostByName(hostName);
    }
    
    public HostInfo (String host, int port) {
    	ArgCheck.isNotNull(host);
		this.hostName = host.toLowerCase();
    	this.portNumber = port;
    	
    	//only cache inetaddresses if they represent the ip. 
    	try {
			InetAddress addr = NetUtils.resolveHostByName(hostName);
			if (addr.getHostAddress().equalsIgnoreCase(hostName)) {
				this.inetAddress = addr;
			}
		} catch (UnknownHostException e) {
		}
    }
    
    public String getHostName() {
        return hostName;
    }

    public int getPortNumber() {
        return portNumber;
    }
    
	public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(hostName).append(":").append(portNumber); //$NON-NLS-1$
        return sb.toString();
    }
    
    /** 
     * @see java.lang.Object#equals(java.lang.Object)
     * @since 4.2
     */
    public boolean equals(Object obj) {
    	if (obj == this) {
    		return true;
    	}
    	if (!(obj instanceof HostInfo)) {
    		return false;
    	}
        HostInfo hostInfo = (HostInfo) obj;
        return hostName.equals(hostInfo.getHostName()) && portNumber == hostInfo.getPortNumber();
    }

    /** 
     * @see java.lang.Object#hashCode()
     * @since 4.2
     */
    public int hashCode() {
        int hc = HashCodeUtil.hashCode(0, hostName);
        return HashCodeUtil.hashCode(hc, portNumber);
    }

}
