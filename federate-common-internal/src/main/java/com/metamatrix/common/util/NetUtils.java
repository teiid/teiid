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

package com.metamatrix.common.util;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.StringTokenizer;


public class NetUtils {

    public static final String DOT_DELIMITER = "."; //$NON-NLS-1$
    public static final String SLASH_DELIMITER = "/"; //$NON-NLS-1$
    public static final String COMMA_DELIMITER = ","; //$NON-NLS-1$
    public static final String COLON_DELIMITER = ":";  //$NON-NLS-1$
    public static final String BACKSLASH_DELIMITER = "\\"; //$NON-NLS-1$
    

    public static final String METAMATRIX_PROTOCOL = "mm"; //$NON-NLS-1$
    public static final String SECURE_METAMATRIX_PROTOCOL = "mms"; //$NON-NLS-1$

    public static final String STANDALONE = CommonPropertyNames.STANDALONE_PLATFORM;
    public static final String APPSERVER_PROPERTY = CommonPropertyNames.SERVER_PLATFORM;
    
    private static String hostName = null;
    private static String hostAddress = null;
    
    /**
     * Used to create the url from the host and port.
     * @param appHostName is the name or ip address of the host to connect to
     * @param appHostPort is the port for the host connection
     * @return
     */
    
    public static String createAppServerURL(String appHostName,
                                            String appHostPort) {
        
        return (appHostName + ":" + appHostPort); //$NON-NLS-1$
    }
    

    /**
     * Returns the short host name by obtaining the first node from the
     * fully qualified host name, assuming there are multilple nodes seperated
     * by ".".
     * @return
     * @throws UnknownHostException
     * @since 4.3
     */
    public static synchronized String getHostname() throws UnknownHostException {
        if (hostName != null) return hostName;
        
        resolveHostName();
        return hostName;

    }
    
    /**
     * Returns the first node of the fully qualified host name   
     * @return
     * @throws UnknownHostException
     * @since 4.3
     */
    public static synchronized String getHostShortName() throws UnknownHostException {
        if (hostName == null  ) {
            return getHostShortName(InetAddress.getLocalHost().getHostName()); 
        } 
            return getHostShortName(hostName); 
    }
    
    /**
     * Returns the first node of the fully qualified host name   
     * @return
     * @throws UnknownHostException
     * @since 4.3
     */
    public static synchronized String getHostShortName(String hostname) throws UnknownHostException {
        StringTokenizer tokens = new StringTokenizer(hostname, "."); //$NON-NLS-1$
        return tokens.nextToken();
    }    
    
    /*
     * Dynamically resolving the host name should only be done when setupmm is being run
     * or when the vm initially starts up and the configuration Host has to be found based on that resolution.  
     * After that, the {@link VMNaming} class should be used to obtain the logical and physical host addresses. 
     */
    private static synchronized void resolveHostName() throws UnknownHostException {
        UnknownHostException unhe =null;        
        InetAddress inet = null;
        try {
            inet = InetAddress.getLocalHost();
            hostName = inet.getCanonicalHostName();
            hostAddress = inet.getHostAddress();
            if (hostName != null) {
                return;
            }

        } catch(UnknownHostException e) {
            unhe = e;
        }
        
        try {
            inet = getFirstNonLoopbackAddress();
            hostName = inet.getCanonicalHostName();
            hostAddress = inet.getHostAddress();

        } catch(SocketException e) {
            if (unhe != null) {
                throw unhe;
            }
            throw new UnknownHostException(e.getMessage());
        }     
        
    }

    /*
     * The {@link VMNaming} class is responsible for setting the host name for its vm.     
     */
    static synchronized void setHostName(String name) {
        hostName = name;
    }
    
    public synchronized static String getHostAddress() throws UnknownHostException {
        if (hostAddress != null) {
            return hostAddress;
        }
        
        resolveHostName();
        
        return hostAddress;
    }

    public static String getFilename(String uri) {
        if (uri == null || uri.length() == 0) {
            return uri;
        }

        //Parse for last instance of "/" to get the MetaModel file name
        int index = uri.lastIndexOf(SLASH_DELIMITER);
        if (index != -1) {
            ++index; // go to the next index after the delimiter
            return uri.substring(index).trim();
        }

        return uri.trim();
    }

    public static String getFilenameWithoutSuffix(String uri) {
        if (uri == null || uri.length() == 0) {
            return uri;
        }

        String filename = getFilename(uri);

        //Parse the file name to remove the extension and then compute the displayable form
        int index = filename.indexOf(DOT_DELIMITER);
        if (index != -1) {
            return filename.substring(0, index);
        }
        return filename;
    }

    


    public static String parseHost(String serverURL) {

        int portIndex = serverURL.lastIndexOf(COLON_DELIMITER); 
        if (portIndex >= 0) {
        } else {
            portIndex = serverURL.length();
        }

        int urlProtocolIndex = serverURL.lastIndexOf(SLASH_DELIMITER); 
        if (urlProtocolIndex < 0) {
            urlProtocolIndex = serverURL.lastIndexOf(BACKSLASH_DELIMITER); 
        }
        if (urlProtocolIndex < 0) {
            return ""; //$NON-NLS-1$
        }
        return serverURL.substring(urlProtocolIndex + 1, portIndex);

    }
    

    public static String parsePort(String serverURL) {
        int portIndex = serverURL.lastIndexOf(COLON_DELIMITER); 
        if (portIndex >= 0) {
            return serverURL.substring(portIndex + 1);
        }
        return ""; //$NON-NLS-1$

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
    public static boolean isPortAvailable(String host, int port) throws UnknownHostException {
        
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
    
    public static InetAddress getFirstNonLoopbackAddress() throws SocketException {
        InetAddress firstipv4=null;
        InetAddress firstipv6=null;
        Enumeration en=NetworkInterface.getNetworkInterfaces();
//        boolean preferIpv4=Boolean.getBoolean("java.net.preferIPv4Stack"); //$NON-NLS-1$
        boolean preferIPv6=Boolean.getBoolean("java.net.preferIPv6Addresses");//$NON-NLS-1$
        while(en.hasMoreElements()) {
            NetworkInterface i=(NetworkInterface)en.nextElement();
            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr=(InetAddress)en2.nextElement();
                if(!addr.isLoopbackAddress()) {
                    if(addr instanceof Inet4Address) {
                        if (firstipv4==null) {
                            firstipv4=addr;
                        }
                        if(preferIPv6)
                            continue;
                        // either prefer ipv4 or dont have preference
                        return addr;
                    }
                    if(addr instanceof Inet6Address) {
                        if (firstipv6==null) {
                            firstipv6=addr;
                        }                        
                        if(preferIPv6)
                            return addr;
                    }
                }
            }
        }        
        if (firstipv4 != null) {
            return firstipv4;
        }
        
        if (firstipv6 != null) {
            return firstipv6;
        }        
        
        return null;
    }

    public static List getAllAvailableInterfaces() throws SocketException {
        List retval=new ArrayList(10);
        NetworkInterface intf;
        for(Enumeration en=NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
            intf=(NetworkInterface)en.nextElement();
            retval.add(intf);
        }
        return retval;
    }    

}
