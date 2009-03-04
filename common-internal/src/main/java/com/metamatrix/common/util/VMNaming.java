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

package com.metamatrix.common.util;

import java.net.InetAddress;
import java.net.UnknownHostException;


public final class VMNaming {
	private static final String HOSTNAME = "HOSTNAME"; //$NON-NLS-1$

    /* 
     * CONFIG_NAME refers to the name used to look up the host in the configuration
     */
    private static String CONFIG_NAME = "";//$NON-NLS-1$

    /*
     * HOST_ADDRESS refers to to the host-name/ip, that is given to clients to connect where
     * the server is. So, in case of Firewall, this may be firewall name, but the bind_address 
     * would be the physical address of the server
     */
    private static InetAddress HOST_ADDRESS = null;
    
    /*
     * BIND_ADDRESS refers to the address used by listeners.  This would include
     * the socket listeners and JGroups.
     */
    private static String BIND_ADDRESS = "";//$NON-NLS-1$
    
    /*
     * Process Name refers to the name of the process that is currently running.
     */
    private static String PROCESS_NAME = "";//$NON-NLS-1$

    
    public static String getProcessName() {
        return PROCESS_NAME;
    }
    
    public static void setProcessName(String processName) {
    	PROCESS_NAME = processName;
    }
    
    public static String getConfigName() {
        return CONFIG_NAME;
    }
    
    public static InetAddress getHostAddress() {
        return HOST_ADDRESS;
    }
    
    public static String getBindAddress() {
        return BIND_ADDRESS;
    }     
   
    public static void setup(String configName, String hostName, String bindAddress) throws UnknownHostException {
    	CONFIG_NAME = configName;
    	
    	boolean bindAddressDefined = (bindAddress != null && bindAddress.length() > 0);
    	boolean hostNameDefined = (hostName != null && hostName.length() > 0);

    	    	
    	if (bindAddressDefined && hostNameDefined) {
    		BIND_ADDRESS = bindAddress;
    		HOST_ADDRESS = NetUtils.resolveHostByName(hostName);
    	}
    	else if (bindAddressDefined && !hostNameDefined) {
    		BIND_ADDRESS = bindAddress;
    		HOST_ADDRESS = InetAddress.getByAddress(BIND_ADDRESS.getBytes());
    	}
    	else if (!bindAddressDefined && hostNameDefined) {
    		HOST_ADDRESS = NetUtils.resolveHostByName(hostName);
    		BIND_ADDRESS = HOST_ADDRESS.getCanonicalHostName();
    	}
    	else {
    		InetAddress addr = NetUtils.getInstance().getInetAddress();
    		BIND_ADDRESS = addr.getHostAddress();
    		HOST_ADDRESS = addr;
    	}
    }
    
    /**
     * Return the stringified representation of this application information object.
     * @return the string form of this object; never null
     */
    public static String getHostInfo() {
        StringBuffer sb = new StringBuffer("Host Information"); //$NON-NLS-1$ 
        sb.append('\n');
        sb.append(" VM Name:               " + PROCESS_NAME ); //$NON-NLS-1$
        sb.append('\n');
        sb.append(" Hostname:              " + HOST_ADDRESS.getCanonicalHostName() ); //$NON-NLS-1$
        sb.append('\n');
        sb.append(" Version:               ").append(ApplicationInfo.getInstance().getReleaseNumber()); //$NON-NLS-1$
        sb.append('\n');
        sb.append(" Build Date:            ").append(ApplicationInfo.getInstance().getBuildDate()); //$NON-NLS-1$
        return sb.toString();
    }
    
	public static String getDefaultConfigName() {
		String nvalue;
		nvalue = System.getenv(HOSTNAME); 
		if (nvalue == null) {
			try {
				nvalue = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				nvalue = "teiid-system"; //$NON-NLS-1$
			}
		}
		return nvalue;
	}
}
