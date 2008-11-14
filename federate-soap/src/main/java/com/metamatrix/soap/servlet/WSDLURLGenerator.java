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

package com.metamatrix.soap.servlet;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.metamatrix.common.util.WSDLServletUtil;
import com.metamatrix.core.log.FileLogWriter;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.soap.SOAPPlugin;
import com.metamatrix.soap.util.ErrorMessageKeys;
import com.metamatrix.soap.util.SOAPConstants;

/**
 * Servlet to build the WSDL URL for the specified VDB
 * 
 * @since 4.2
 */
public class WSDLURLGenerator extends HttpServlet {

    public LogListener newListener = null;
    public FileLogWriter logWriter = null;

    MMGetVDBResourcePlatformLog platformLog = MMGetVDBResourcePlatformLog.getInstance();
    
    synchronized public void init(ServletConfig config) throws ServletException {
        super.init(config);

        String logFile = getServletContext().getInitParameter("logfile"); //$NON-NLS-1$

        File log = new File(logFile);
        logWriter = new FileLogWriter(log);
        platformLog.getPlatformLog().addListener(logWriter);
    }

    public void doGet(HttpServletRequest req,
                      HttpServletResponse resp) throws ServletException,
                                               IOException {
        doPost(req, resp);
    }

    /**
     * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     * @since 4.2
     */
    public void doPost(HttpServletRequest req,
                       HttpServletResponse resp) throws ServletException,
                                                IOException {

        // If we got here, this is a request for the WSDL URL of a VDB
        String mmServerHostAndPortList = req.getParameter(WSDLServletUtil.MMSERVER_HOST_PORT_KEY);
        String vdbName = req.getParameter(WSDLServletUtil.VDB_NAME_KEY);
        String vdbVersion = req.getParameter(WSDLServletUtil.VDB_VERSION_KEY);
        String targetHost = req.getParameter(WSDLServletUtil.TARGET_HOST_KEY);
        String targetPort = req.getParameter(WSDLServletUtil.TARGET_PORT_KEY);
        String scheme = req.getScheme();

        // Validate parameters
        List hostAndPortList = Collections.EMPTY_LIST;
        try {
            hostAndPortList = checkHostAndPortFormValue(mmServerHostAndPortList, WSDLServletUtil.MMSERVER_HOST_PORT_KEY);
            checkFormValue(vdbName, WSDLServletUtil.VDB_NAME_KEY);
            checkFormValue(targetHost, WSDLServletUtil.TARGET_HOST_KEY);

        } catch (Exception e) {
            MMGetVDBResourcePlatformLog.getInstance().getLogFile().log(MessageLevel.ERROR, e, e.getMessage());
            resp.getOutputStream().println(e.getMessage());
            return;
        }
        
        // Convert the server host and port list into two equally-sized (and validated) lists
        List mmServers = new ArrayList(hostAndPortList.size());
        List mmPorts = new ArrayList(hostAndPortList.size());
        Iterator iter = hostAndPortList.iterator();
        while (iter.hasNext()) {
        	String hostAndPort = (String)iter.next();
        	// Split the host:port at the colon
        	int colonIndex = hostAndPort.indexOf(':');
        	String mmServer = hostAndPort.substring(0,colonIndex);
        	String mmPort = hostAndPort.substring(colonIndex+1);
        	// Validate that the port is an integer
            if (!validateInteger(resp, mmPort, ErrorMessageKeys.SERVICE_0020)) {
                return;
            }
            mmServers.add(mmServer);
            mmPorts.add(mmPort);
        }

        if (targetPort!=null && targetPort.length()>0 && !validateInteger(resp, targetPort, ErrorMessageKeys.SERVICE_0020)) {
            return;
        }

        if (vdbVersion != null && vdbVersion.length()>0 && !validateInteger(resp, vdbVersion, ErrorMessageKeys.SERVICE_0024)) {
            return;
        }

        // Check URLs for all mmServerHost and mmServerPort combinations
    	String mmServerHost = null;
        try {
        	for(int i=0; i<mmServers.size(); i++) {
        		mmServerHost = (String)mmServers.get(i);
        		new URL(scheme+"://" + mmServerHost + ":" + mmPorts.get(i)); //$NON-NLS-1$ //$NON-NLS-2$
        	}
        } catch (MalformedURLException mue) {
            String message = SOAPPlugin.Util.getString(ErrorMessageKeys.SERVICE_0021, mmServerHost);
            MMGetVDBResourcePlatformLog.getInstance().getLogFile().log(MessageLevel.ERROR, mue, message);
            resp.getOutputStream().println(message);
            return;
        }
        
        // Added encoding to maintain proper URL formatting protocol.  Server URL has reserved characters in it.
        // Create list of encoded URLs
        List serverURLs = new ArrayList(mmServers.size());
        for(int i=0; i<mmServers.size(); i++) {
        	final String serverURL = URLEncoder.encode(getProtocol(req.getParameter(WSDLServletUtil.SECURE_PROTOCOL)) + "://" + (String)mmServers.get(i) + ":" + (String)mmPorts.get(i), "UTF-8"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        	// First time, add entire url, including protocol
        	if(i==0) {
        		serverURLs.add(serverURL);
            // Subsequent additions are just the host and port
        	} else {
        		String serverAndPortStr = (String)mmServers.get(i) + ":" + (String)mmPorts.get(i); //$NON-NLS-1$
        		serverURLs.add(URLEncoder.encode(serverAndPortStr,"UTF-8")); //$NON-NLS-1$ 
        	}
        }

        if(targetPort==null) targetPort = ""; //$NON-NLS-1$
        String servletPath = WSDLServletUtil.formatURL(scheme,
        											   targetHost,
									                   targetPort,
									                   serverURLs, 
									                   vdbName,
									                   vdbVersion);        
        URL url = new URL(servletPath);
        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        // if there was an error, show the message to the user
        if (connection==null || MMGetVDBResourceServlet.WSDL_ERROR.equals(connection.getHeaderField(MMGetVDBResourceServlet.WSDL_ERROR))) {
            StringBuffer error = new StringBuffer();
            InputStreamReader reader = new InputStreamReader(connection.getInputStream());
            try {
                int c;
                while ((c = reader.read()) != -1) {
                    error.append((char)c);
                }
                String message = SOAPPlugin.Util.getString(ErrorMessageKeys.SERVICE_0022, servletPath, error.toString());
                MMGetVDBResourcePlatformLog.getInstance().getLogFile().log(MessageLevel.ERROR, message);
                resp.getOutputStream().println(message);
            } finally {
                reader.close();
            }
            return;
        }

        resp.getOutputStream().println("<a href='" + servletPath + "'>" + servletPath + "</a>"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    /**
     * @param resp
     * @param serverPort
     * @throws IOException
     */
    private boolean validateInteger(HttpServletResponse resp,
                                    String integer,
                                    String error_key) throws IOException {
        try {
            int port = Integer.parseInt(integer);
            if (port < 0) {
                throw new NumberFormatException(SOAPPlugin.Util.getString(ErrorMessageKeys.SERVICE_0023)); 
            }
        } catch (NumberFormatException nfe) {
            String message = SOAPPlugin.Util.getString(error_key, integer);
            MMGetVDBResourcePlatformLog.getInstance().getLogFile().log(MessageLevel.ERROR, nfe, message);
            resp.getOutputStream().println(message);
            return false;
        }
        return true;
    }

    /**
     * Get the protocol to use for this MetaMatrix connection. If the user checked the secure protocol checlbox on the WSDL Url
     * Generator, we will return the secure protocol. Otherwise, we will return the standard url protocol.
     */
    private String getProtocol(String secure) {

        if (secure != null && secure.equals(WSDLServletUtil.SECURE_PROTOCOL)) {
            return SOAPConstants.SECURE_PROTOCOL;
        }

        return SOAPConstants.NON_SECURE_PROTOCOL;
    }

    /**
     * Internal helper method to verify that a form value has data behind it
     * 
     * @throws Exception
     *             if this is not the case
     */
    private static void checkFormValue(String parameter,
                                       String expectedParameterName) throws Exception {
        if (parameter == null || parameter.trim().length() == 0) {
            throw new Exception(SOAPPlugin.Util.getString(ErrorMessageKeys.SERVICE_0004, expectedParameterName));
        }
    }
    
    /**
     * Internal helper method to verify that the Host and Port form value has correctly formatted data behind it.
     * The supplied parameter may be a single Host:Port, or it may be a comma-delimited string of Host:Port combinations.
     * This method verifies that the string is non-null and non-zero length, and also that it is formatted correctly
     * 
     * @throws Exception
     *             if this is not the case
     */
    private static List checkHostAndPortFormValue(String parameter,
                                       String expectedParameterName) throws Exception {
    	
    	List hostPortList = new ArrayList();
    	// First, check that the supplied parameter is non-null and not empty
        if (parameter == null || parameter.trim().length() == 0) {
            throw new Exception(SOAPPlugin.Util.getString(ErrorMessageKeys.SERVICE_0004, expectedParameterName));
        }
        
        // Second, check that it is formatted correctly
        StringTokenizer st = new StringTokenizer(parameter,",",false);  //$NON-NLS-1$
        while(st.hasMoreTokens()) {
        	String hostPortToken = st.nextToken();
        	if(hostPortToken!=null && hostPortToken.trim().length()!=0) {
        		String hostPortStr = hostPortToken.trim();
        		if(hostPortStr.indexOf(':')==-1) {
                    throw new Exception(SOAPPlugin.Util.getString(ErrorMessageKeys.SERVICE_0025, hostPortToken));
        		}
        		hostPortList.add(hostPortStr);
        	}
        }
        
        // Check the Host : Port list to make sure there is at least one
        if(hostPortList.isEmpty()) {
            throw new Exception(SOAPPlugin.Util.getString(ErrorMessageKeys.SERVICE_0026));
        }
        
        return hostPortList;
    }
    
}
