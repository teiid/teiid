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

package com.metamatrix.soap.servlet;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.metamatrix.common.util.WSDLServletUtil;
import com.metamatrix.core.log.FileLogWriter;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.soap.SOAPPlugin;
import com.metamatrix.soap.util.ErrorMessageKeys;
import com.metamatrix.soap.util.SOAPConstants;
import com.metamatrix.soap.util.WebServiceUtil;

/**
 * Servlet to retrieve WSDL from a VDB using servlet context values specified in
 * the web.xml.
 * 
 * @since 5.6
 */
public class MMGetWSDLServlet extends MMGetVDBResourceServlet {

	/** MM Server host/port/protocol */
	private String mmServer = StringUtil.Constants.EMPTY_STRING;
	private String mmProtocol = StringUtil.Constants.EMPTY_STRING;

	public MMGetWSDLServlet() {
	}

	synchronized public void init(ServletConfig config) throws ServletException {
		super.init(config);

		String logFile = getServletContext().getInitParameter("logfile"); //$NON-NLS-1$
		File log = new File(logFile);
		logWriter = new FileLogWriter(log);
		platformLog.getPlatformLog().addListener(logWriter);

		mmServer = getServletContext().getInitParameter("mmServer"); //$NON-NLS-1$
		mmProtocol = getServletContext().getInitParameter("mmProtocol"); //$NON-NLS-1$        
	}

	/**
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest,
	 *      javax.servlet.http.HttpServletResponse)
	 * @since 5.6
	 */
	public void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {

		Connection connection = null;
		
		/** Web Server host/port/protocol */
		String webServer = StringUtil.Constants.EMPTY_STRING;
		int webPortInt = 0;
		String webProtocol = StringUtil.Constants.EMPTY_STRING;
		
		webServer = req.getServerName();
		webPortInt = req.getServerPort();
		webProtocol = req.getScheme();

		/** VDB Name and Version */
		String vdbName = StringUtil.Constants.EMPTY_STRING;
		String vdbVersion = StringUtil.Constants.EMPTY_STRING;

		vdbName = getVdbName(req.getPathInfo());
		vdbVersion = getVdbVersion(req.getPathInfo());

		// Set the default content type. If we get a resource, we will change
		// this to text/xml.
		resp.setContentType(WSDLServletUtil.DEFAULT_CONTENT_TYPE);

		// set error in header, clear after successful wsdl return
		resp.setHeader(WSDL_ERROR, WSDL_ERROR);

		// Build server URL
		StringBuffer serverURL = new StringBuffer();
		serverURL.append(mmProtocol)
				.append("://").append(mmServer); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		String resourcePath = "/MetaMatrixDataServices.wsdl"; //$NON-NLS-1$ 

		serverURL.append(";").append(SOAPConstants.APP_NAME).append("="); //$NON-NLS-1$ //$NON-NLS-2$
		serverURL.append(SOAPPlugin.Util
				.getString("MMGetVDBResourceServlet.Application_Name")); //$NON-NLS-1$        

		try {
			connection = getConnection(WebServiceUtil.WSDLUSER, WebServiceUtil.WSDLPASSWORD, vdbName, vdbVersion,
					serverURL.toString());
		} catch (Exception e) {
			String message = SOAPPlugin.Util
					.getString(ErrorMessageKeys.SERVICE_0006); // ;
			MMGetVDBResourcePlatformLog.getInstance().getLogFile().log(
					MessageLevel.ERROR, e, message);
			resp.getOutputStream().println(message);
			return;
		}

		/*
		 * Need to create a string of server properties to set as the prefix for
		 * the action value. The suffix will be the procedure name in the form
		 * of "procedure=fully.qualified.procedure.name".
		 */
		StringBuffer serverProperties = new StringBuffer();

		serverProperties.append(WSDLServletUtil.SERVER_URL_KEY).append(EQUALS)
				.append(serverURL.substring(0, serverURL.indexOf(";"))); //$NON-NLS-1$
		serverProperties.append(AMP).append(WSDLServletUtil.VDB_NAME_KEY)
				.append(EQUALS).append(vdbName);
		if (vdbVersion != null
				&& !vdbVersion.equals(StringUtil.Constants.EMPTY_STRING)) {
			serverProperties.append(AMP)
					.append(WSDLServletUtil.VDB_VERSION_KEY).append(EQUALS)
					.append(vdbVersion);
		}
		
		
		StringBuffer suffix = new StringBuffer();
		suffix.append(serverProperties.toString());
		
		serverProperties.append(AMP);
		serverProperties = new StringBuffer(
				escapeAttributeEntities(serverProperties.toString()));

		StringBuffer urlPrefix = new StringBuffer();
		urlPrefix.append(webProtocol)
				.append("://").append(webServer).append(":").append(webPortInt); //$NON-NLS-1$ //$NON-NLS-2$ 

		String servletPath = urlPrefix + "/" + WSDLServletUtil.SERVLET_PATH; //$NON-NLS-1$ 

		String result = escapeAttributeEntities(suffix.toString());
		
		result = "?" + result; //$NON-NLS-1$
		
		String endPointURL = null;

		endPointURL = urlPrefix + DATASERVICE;

		try {
			getResource(resp, procString, resourcePath, servletPath, result,
					serverProperties.toString(), endPointURL, connection);

		} catch (SQLException se) {
			resp.getOutputStream().println(se.getMessage());
			MMGetVDBResourcePlatformLog.getInstance().getLogFile().log(
					MessageLevel.ERROR, se,
					SOAPPlugin.Util.getString("MMGetVDBResourceServlet.7")); //$NON-NLS-1$            
		} catch (Exception e) {
			MMGetVDBResourcePlatformLog.getInstance().getLogFile().log(
					MessageLevel.ERROR, e,
					SOAPPlugin.Util.getString("MMGetVDBResourceServlet.8")); //$NON-NLS-1$                                                                    
			resp.getOutputStream().println(e.getMessage());
		} finally {
			try {
				// Cleanup our connection
				connection.close();
			} catch (SQLException e) {
				MMGetVDBResourcePlatformLog.getInstance().getLogFile().log(
						MessageLevel.ERROR, e,
						SOAPPlugin.Util.getString("MMGetVDBResourceServlet.0")); //$NON-NLS-1$
				resp.setHeader(WSDL_ERROR, WSDL_ERROR);
				resp.getOutputStream().println(e.getMessage());
			}
		}

	}

	/**
	 * This will derive the vdb name from the path of the URL used to obtain the
	 * VDB resource.
	 * 
	 * The format of the path is: wsdl/vdbName/vdbVersion
	 * 
	 * @param path -
	 *            path from the URL used to obtain the VDB resource.
	 * @return vdbName
	 * @since 5.6
	 */
	public static String getVdbName(String path) {
		String vdbName = StringUtil.Constants.EMPTY_STRING;
		int start = path.indexOf("/") + 1; //$NON-NLS-1$ 
		int end = path.indexOf("/", start); //$NON-NLS-1$ 
		// if version was left off, just go to the end
		if (end == -1) {
			end = path.length();
		}
		vdbName = path.substring(start, end);
		return vdbName;
	}

	/**
	 * This will derive the vdb version from the path of the URL used to obtain
	 * the VDB resource. The version is not required. If it is not present, the
	 * latest version will be assumed.
	 * 
	 * The format of the path is: wsdl/vdbName/vdbVersion
	 * 
	 * @param path -
	 *            path from the URL used to obtain the VDB resource.
	 * @return vdbVersion
	 * @since 5.6
	 */
	public static String getVdbVersion(String path) {
		String vdbVersion = StringUtil.Constants.EMPTY_STRING;
		int start = path.indexOf("/") + 1; //$NON-NLS-1$
		start = path.indexOf("/", start); //$NON-NLS-1$
	
		// if version was left off, just return
		if (start == -1) {
			return vdbVersion;
		}
	
		vdbVersion = path.substring(start + 1, path.length());
		return vdbVersion;
	}
		
}
