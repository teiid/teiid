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

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;


public class NetUtils {
	private InetAddress inetAddress;
	
	private static NetUtils INSTANCE = new NetUtils();
	
	public static NetUtils getInstance() {
		return INSTANCE;
	}
	    
    public InetAddress getInetAddress() throws UnknownHostException {
    	resolveHostName();
    	return this.inetAddress; 
    }
    

    /**
     * Resolves the given host name into InetAddress; if host name can not be resolved then it will
     * throw {@link UnknownHostException}
     * @param hostName
     * @return
     * @throws UnknownHostException
     */
    public static InetAddress resolveHostByName(String hostName) throws UnknownHostException {
        if( hostName.equalsIgnoreCase("localhost")) { //$NON-NLS-1$
            try {
				return getInstance().getInetAddress();
			} catch (UnknownHostException e) {
			}
        } 
       	return InetAddress.getByName(hostName);
    }
    
    /*
     * Dynamically resolving the host name should only be done when setupmm is being run
     * or when the vm initially starts up and the configuration Host has to be found based on that resolution.  
     * After that, the {@link VMNaming} class should be used to obtain the logical and physical host addresses. 
     */
    private synchronized void resolveHostName() throws UnknownHostException {
    	UnknownHostException une = null;

    	boolean preferIPv6=Boolean.getBoolean("java.net.preferIPv6Addresses");//$NON-NLS-1$

    	// majority of the times we will find the address with this below call 
    	if (this.inetAddress == null) {
	    	try {
	        	InetAddress addr = InetAddress.getLocalHost();
	        	if(!addr.isLoopbackAddress()) {
	        		this.inetAddress = addr;
	        	}
	        } catch(UnknownHostException e) {
	        	une=e;
	        }     
    	}

    	// see if you can find a non-loopback address, based on the preference
    	if (this.inetAddress == null) {
    		this.inetAddress = findAddress(preferIPv6, false);
    	}
    	
    	// if no-addresses found so far then resort to IPv4 loopback address
    	if (this.inetAddress == null) {
    		this.inetAddress = findAddress(false, true);
    	}
    	
    	if (this.inetAddress == null) {
    		if (une != null) throw une;
    		throw new UnknownHostException("failed to resolve the address for localhost"); //$NON-NLS-1$
    	}
    }

    
    
    /**
     * Finds a InetAddress of the current host where the JVM is running, by querying NetworkInterfaces installed
     * and filters them by given preferences. It will return the first Address which UP and meets the criteria
     * @param preferIPv6 
     * @param perferLoopback
     * @return null is returned if requested criteria is not met.
     * @throws UnknownHostException
     * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4665037 (Linux issue with resolving to loopback in the DHCP situations)
     */
    private InetAddress findAddress(boolean preferIPv6, boolean perferLoopback) throws UnknownHostException {
    	try {
			Enumeration<NetworkInterface> ne = NetworkInterface.getNetworkInterfaces();
			while (ne.hasMoreElements()) {
				NetworkInterface ni = ne.nextElement();
				//## JDBC4.0-begin ##
				if (ni.isUp()) {
				//## JDBC4.0-end ##
					Enumeration<InetAddress> addrs = ni.getInetAddresses();
					while (addrs.hasMoreElements()) {
						InetAddress addr = addrs.nextElement();
						
						boolean isIPv6 = (addr instanceof Inet6Address);
						if (preferIPv6 == isIPv6 && perferLoopback == addr.isLoopbackAddress() ) {
							return addr;
						}
					}
				//## JDBC4.0-begin ##		
				}
				//## JDBC4.0-end ##
			}
		} catch (SocketException e) {
			// treat this as address not found and return null;
		}
    	return null;
    }
    
    /**
     * Call to determine if a port is available to be opened.  
     * This is used to determine if a port is already opened 
     * by some other process. If the port is available, then
     * it's not in use.   
     * @param host
     * @param port
     * @return true if the port is not opened.
     * @since 4.3
     */
    public boolean isPortAvailable(String host, int port) throws UnknownHostException {
        
        try {
            //using Socket to try to connect to an existing opened socket
            Socket ss = new Socket(host, port);
            
            try {
                ss.close();
            } catch (Exception ce) {
                // it was open and considered available, then dont worry about the close error
            }
            return false;
        } catch (UnknownHostException ce) {
            throw ce;
        } catch (IOException e) {
        	//ignore
        }
        return true;
    }
}
