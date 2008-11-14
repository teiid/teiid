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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import com.metamatrix.common.CommonPlugin;

/**
 * Class to encapsulate URL to a Clustered Metamatrix Server with multiple host
 * 
 * @since 4.2
 */
public class MMURL {

    public static final String DOT_DELIMITER = "."; //$NON-NLS-1$
    public static final String DOUBLE_SLASH_DELIMITER = "//"; //$NON-NLS-1$
    public static final String COMMA_DELIMITER = ","; //$NON-NLS-1$

    public static final String COLON_DELIMITER = ":"; //$NON-NLS-1$
    public static final String BACKSLASH_DELIMITER = "\\"; //$NON-NLS-1$
    public static final String DEFAULT_PROTOCOL= MMURL_Properties.SERVER.NON_SECURE_PROTOCOL + "://"; //$NON-NLS-1$
    public static final String SECURE_PROTOCOL= MMURL_Properties.SERVER.SECURE_PROTOCOL + "://"; //$NON-NLS-1$

    public static final String FORMAT_SERVER = CommonPlugin.Util.getString("MMURL.FORMAT_SERVER"); //$NON-NLS-1$

    public static final String INVALID_FORMAT_SERVER = CommonPlugin.Util.getString("MMURL.INVALID_FORMAT", new Object[] {FORMAT_SERVER}); //$NON-NLS-1$


    
    /*
     * appserver URL
     */
    private String appServerURL;
    /*
     * List of <code> HostData </code> in a cluster to connect to Matamatrix
     */
    private List hosts = new ArrayList();

    private boolean usingSSL = false;
    
    /**
     * Create an MMURL from the server URL.  For use by the server-side.
     * @param serverURL   Expected format: mm[s]://server1:port1[,server2:port2]
     * @since 4.2
     */
    public MMURL(String serverURL) {
        appServerURL = serverURL;
        if (!hasValidURLProtocol(serverURL)) {
            throw new IllegalArgumentException(INVALID_FORMAT_SERVER);
        }
        usingSSL = isSecureProtocol(appServerURL);
		parseURL(serverURL, INVALID_FORMAT_SERVER);
    }
    
    public MMURL(String host, int port, boolean secure) {
        usingSSL = secure;
		hosts.add(new HostInfo(host, port));
    }
    
    /**
     * Validates that a server URL is in the correct format.
     * @param serverURL  Expected format: mm[s]://server1:port1[,server2:port2]
     * @since 4.2
     */
    public static boolean isValidServerURL(String serverURL) {
        boolean valid = true;
        try {
            new MMURL(serverURL);            
        } catch (Exception e) {
            valid = false;
        }
        
        return valid;
    }

    /**
     * @param appServerURL2
     * @return
     */
    private boolean isSecureProtocol(String url) { 
        return url.toLowerCase().startsWith(SECURE_PROTOCOL);
    }
    
    private boolean hasValidURLProtocol(String url) {
        return ( url != null && (url.startsWith(DEFAULT_PROTOCOL)) || url.startsWith(SECURE_PROTOCOL));        
    }
    
    public List getHostInfo() {
        return hosts;
    }
    
    /**
     * Get a list of hosts
     *  
     * @return string of host seperated by commas
     * @since 4.2
     */
    public String getHosts() {
        StringBuffer hostList = new StringBuffer("");  //$NON-NLS-1$
        if( hosts != null) {
            Iterator iterator = hosts.iterator();
            while (iterator.hasNext()) {
                HostInfo element = (HostInfo)iterator.next();
                hostList.append(element.getHostName());
                if( iterator.hasNext()) { 
                    hostList.append(COMMA_DELIMITER); 
                }
            }
        }
        return hostList.toString();
    }
    
    /**
     * Get a list of ports  
     * 
     * @return string of ports seperated by commas
     * @since 4.2
     */
    public String getPorts() {
        StringBuffer portList = new StringBuffer("");  //$NON-NLS-1$
        if( hosts != null) {
            Iterator iterator = hosts.iterator();
            while (iterator.hasNext()) {
                HostInfo element = (HostInfo)iterator.next();
                portList.append(element.getPortNumber());
                if( iterator.hasNext()) { 
                    portList.append(COMMA_DELIMITER); 
                }
            }
        }
        return portList.toString();
    }

    private void parseURL(String url, String exceptionMessage) {

        //      Parse property string
        int urlProtocolIndex = url.indexOf(DOUBLE_SLASH_DELIMITER);
        String serverURL;
        if (urlProtocolIndex > 0) {
            serverURL = url.substring(urlProtocolIndex + 2);
            if(serverURL == null || serverURL.equals("")) { //$NON-NLS-1$
                throw new IllegalArgumentException(exceptionMessage);
            }
			parseServerURL(serverURL, exceptionMessage);
        } else {
            throw new IllegalArgumentException(exceptionMessage);
        }
    }

    /**
     * @param url
     * @throws UnknownHostException 
     * @since 4.2
     */
    private void parseServerURL(String serverURL, String exceptionMessage) {
        StringTokenizer st;
        StringTokenizer st2;

        st = new StringTokenizer(serverURL, COMMA_DELIMITER); 
        if (!st.hasMoreTokens()) {
            throw new IllegalArgumentException(exceptionMessage);
        }
        while (st.hasMoreTokens()) {
            st2 = new StringTokenizer(st.nextToken(), COLON_DELIMITER);
            try {
                String host = st2.nextToken().trim();
                String port = st2.nextToken().trim();
                HostInfo hostInfo = new HostInfo(host, port);
                hosts.add(hostInfo);
            } catch (NoSuchElementException nsee) {
                throw new IllegalArgumentException(exceptionMessage);
            } catch (NullPointerException ne) {
                throw new IllegalArgumentException(exceptionMessage);
            }
        }
    }

    /**
     * Get the Metamatrix Application Server URL
     * 
     * @return String for connection to the Metamatrix Server
     * @since 4.2
     */
    public String getAppServerURL() {
        if (appServerURL == null) {
            StringBuffer sb = new StringBuffer();
            if (usingSSL) {
                sb.append(SECURE_PROTOCOL);
            } else {
                sb.append(DEFAULT_PROTOCOL);
            }
            Iterator iter = hosts.iterator();
            while (iter.hasNext()) {
                HostInfo host = (HostInfo)iter.next();
                sb.append(host.getHostName());
                sb.append(COLON_DELIMITER); 
                sb.append(host.getPortNumber());
                if (iter.hasNext()) {
                    sb.append(COMMA_DELIMITER);
                }
            }
            appServerURL = sb.toString();
        }
        return appServerURL;
    }

    /**
     * @see java.lang.Object#toString()
     * @since 4.2
     */
    public String toString() {
        return getAppServerURL(); 
    }

    /** 
     * @see java.lang.Object#equals(java.lang.Object)
     * @since 4.2
     */
    public boolean equals(Object obj) {
        boolean same;
        if (obj == null) {
            same = false;
        } else if (obj == this) {
            same = true;
        } else if (!(obj instanceof MMURL)) {
            same = false;
        } else {
            MMURL url = (MMURL)obj;
            same = (appServerURL.equals(url.getAppServerURL()));
        }
        return same;
    }
    
    /** 
     * @see java.lang.Object#hashCode()
     * @since 4.2
     */
    public int hashCode() {
        return appServerURL.hashCode();
    }

	public boolean isUsingSSL() {
		return usingSSL;
	}

    

}
