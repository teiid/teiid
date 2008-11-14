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

package com.metamatrix.common.api;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.metamatrix.core.util.HashCodeUtil;

/**
 * HostInfo is used internally by Metamatirx to store the host information used in creating the Service Object reference
 * 
 * @since 4.2
 */
public class HostInfo {
    // Host Name and Port Number
    private String hostName;
    private InetAddress inetAddress;
    private int portNumber  = 0;

    public HostInfo(String host, String port) {
        this(host, parsePort(port));
    }

	private static int parsePort(String port) {
		try {
            return Integer.parseInt(port);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("port must be numeric:" + port); //$NON-NLS-1$
        }
	}
    
    public HostInfo (String host, int port) {
    	this(host, port, null);
    }
    
    /**
     * @since 4.2
     */
    public HostInfo(String host, int port, InetAddress inetAddress) {
    	if (host == null || host.equals("")) { //$NON-NLS-1$
            throw new IllegalArgumentException("hostname can't be null"); //$NON-NLS-1$
        }
        if( host.equalsIgnoreCase("localhost")) { //$NON-NLS-1$
            try {
				this.hostName = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				this.hostName = host.toLowerCase();
			}
        } else {
        	this.hostName = host.toLowerCase();
        }
        if (inetAddress == null) {
            try {
				this.inetAddress = InetAddress.getByName(hostName);
			} catch (UnknownHostException e) {
				//ignore
			}
        } else {
        	this.inetAddress = inetAddress;
        }
        portNumber = port;
        
        if (portNumber < 0 || portNumber > 0xFFFF) {
            throw new IllegalArgumentException("port out of range:" + portNumber); //$NON-NLS-1$
        }
        if (hostName == null) {
            throw new IllegalArgumentException("hostname can't be null");  //$NON-NLS-1$
        }
    }
    
    public String getHostName() {
        return hostName;
    }

    public void setPortNumber(int thePortNumber) {
        portNumber = thePortNumber;
    }

    public int getPortNumber() {
        return portNumber;
    }

    public InetAddress getInetAddress() {
        return inetAddress;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("HostInfo: "); //$NON-NLS-1$
        sb.append(" hostName:  " + hostName); //$NON-NLS-1$
        sb.append(" portNumber:  " + portNumber); //$NON-NLS-1$
        sb.append(" inetAddress: "); //$NON-NLS-1$
        if (inetAddress != null) {
            sb.append(inetAddress);
        }
        return sb.toString();
    }
    
    /** 
     * @see java.lang.Object#equals(java.lang.Object)
     * @since 4.2
     */
    public boolean equals(Object obj) {
        HostInfo hostInfo = (HostInfo) obj;
        if (inetAddress == null || hostInfo.getInetAddress() == null) {
            return hostName.equals(hostInfo.getHostName()) && portNumber == hostInfo.getPortNumber();
        }
        return inetAddress.equals(hostInfo.getInetAddress()) && portNumber == hostInfo.getPortNumber();
    }

    /** 
     * @see java.lang.Object#hashCode()
     * @since 4.2
     */
    public int hashCode() {
        int hc = 0;

        if (inetAddress != null) {
        	hc = HashCodeUtil.hashCode(hc, inetAddress.getHostAddress());
        } else {
        	hc = HashCodeUtil.hashCode(hc, hostName);
        }
        hc = HashCodeUtil.hashCode(hc, portNumber);
        
        return hc;
    }

}
