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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;

/**
 * Constants pertaining to WSDL servlet execution.
 * 
 * @since 4.2
 */

public class WSDLServletUtil {

	/**
	 * General keys
	 */
	public static final String SERVER_URL_KEY = "ServerURL"; //$NON-NLS-1$

	public static final String SECURE_PROTOCOL = "Secure"; //$NON-NLS-1$

	public static final String VDB_NAME_KEY = "VDBName"; //$NON-NLS-1$

	public static final String VDB_VERSION_KEY = "VDBVersion"; //$NON-NLS-1$

	public static final String ADD_PROPS = "AdditionalProperties"; //$NON-NLS-1$

	public static final String TXN_AUTO_WRAP = "txnAutoWrap"; //$NON-NLS-1$

	public static final String ADD_EXEC_PROPS = "AddExecProperties"; //$NON-NLS-1$

	public static final String MM_WEBSERVICE_QUERY_TIMEOUT = "com.metamatrix.webservice.querytimeout"; //$NON-NLS-1$

	public static final String DISCOVERED_WSDL = "discovered_wsdl"; //$NON-NLS-1$

	/*
	 * This is the parameter that will tell this servlet when the web service endpoint as defined in WSDL served up by this
	 * servlet will use the HTTP vs HTTPS protocol.
	 */
	public static final String HTTP_TYPE_PARAMETER_KEY = "httptype"; //$NON-NLS-1$

	/*
	 * This is the value of the httptype URL request param that will indicate that the returned WSDL should have an http endpoint
	 * instead of an https endpoint.
	 */
	public static final String HTTP_PARAMETER_VALUE = "http"; //$NON-NLS-1$

	/*
	 * Static contant representing the standard http protocol.
	 */
	public static final String HTTP = "http"; //$NON-NLS-1$

	/**
	 * Static contant representing the secure http protocol.
	 */
	public static final String HTTPS = "https"; //$NON-NLS-1$

	/**
	 * Default content type for the VDBResourceServlet
	 */
	public static final String DEFAULT_CONTENT_TYPE = "text/html"; //$NON-NLS-1$

	/**
	 * XML content type for the VDBResourceServlet
	 */
	public static final String XML_CONTENT_TYPE = "text/xml"; //$NON-NLS-1$

	/**
	 * WSDL URL Generator keys
	 */
	public static final String MMSERVER_HOST_PORT_KEY = "MMServerHostAndPort"; //$NON-NLS-1$

	public static final String TARGET_HOST_KEY = "TargetHost"; //$NON-NLS-1$

	public static final String TARGET_PORT_KEY = "TargetPort"; //$NON-NLS-1$

	public static final String SERVLET_PATH = "/servlet/ArtifactDocumentService"; //$NON-NLS-1$

	public static final String SQLQUERYWEBSERVICE_WSDL_PATH = "/services/SqlQueryWebService?wsdl"; //$NON-NLS-1$

	public static final String GENERATED_WSDL_NAME = "MetaMatrixDataServices"; //$NON-NLS-1$

	public static final String GENERATED_WSDL_FILENAME = GENERATED_WSDL_NAME + ".wsdl"; //$NON-NLS-1$

	public static final String COLON = ":"; //$NON-NLS-1$

	public static final String SLASH = "/"; //$NON-NLS-1$

	public static final String DOUBLE_SLASH = "//"; //$NON-NLS-1$

	public static final String AMP = "&"; //$NON-NLS-1$

	public static final String QUESTION_MARK = "?"; //$NON-NLS-1$

	public static final String EQUALS = "="; //$NON-NLS-1$

	public static final String COMMA = ","; //$NON-NLS-1$

	private static final String SQLQUERYWEBSERVICE_URL_FORMAT = "{0}://{1}:{2}/{3}"; //$NON-NLS-1$

	/*
	 * this default value is based on Tomcat's default value in its server.xml file. This value can be overridden by setting the
	 * com.metamatrix.webservice.dataservice.httpsport System property for the VM that this servlet is running in.
	 */
	private static final String DEFAULT_HTTPS_PORT = "8443"; //$NON-NLS-1$

	private static final String DEFAULT_HTTP_PORT = "8080"; //$NON-NLS-1$

	private static final String HTTPS_PORT_PROPERTY_KEY = "com.metamatrix.webservice.dataservice.httpsport"; //$NON-NLS-1$

	private static final String HTTP_PORT_PROPERTY_KEY = "com.metamatrix.webservice.dataservice.httpport"; //$NON-NLS-1$

	/**
	 * Returns the formatted url from the supplied info
	 * 
	 * @param scheme the server scheme
	 * @param host the server host name
	 * @param port the server port
	 * @param appContext the context of this application to use in the WSDL url
	 * @param serverURLs the list of server url info, first url is full url including protocol. Subsequent items are just the
	 *        host:port strings.
	 * @param vdbName the vdb name
	 * @param vdbVersion the vdb version number
	 */
	public static String formatURL( String scheme,
	                                String host,
	                                String port,
	                                String appContext,
	                                List serverURLs,
	                                String vdbName,
	                                String vdbVersion ) {

		StringBuffer result = new StringBuffer();
		try {
			boolean hasPort = true;
			boolean hasVDBVersion = true;

			if (port == null || port.length() == 0) {
				hasPort = false;
			}

			if (vdbVersion == null || vdbVersion.trim().length() == 0) {
				hasVDBVersion = false;
			}

			result.append(scheme).append(COLON).append(DOUBLE_SLASH).append(host);

			if (hasPort) {
				result.append(COLON).append(port);
			}

			result.append(appContext).append(SERVLET_PATH).append(SLASH).append(GENERATED_WSDL_FILENAME);
			result.append(QUESTION_MARK).append(SERVER_URL_KEY).append(EQUALS);
			// Append comma-delimited server urls
			Iterator iter = serverURLs.iterator();
			while (iter.hasNext()) {
				String serverURL = (String)iter.next();
				result.append(serverURL);
				// If there is another url coming, add an encoded comma
				if (iter.hasNext()) {
					result.append(URLEncoder.encode(COMMA, "UTF-8")); //$NON-NLS-1$
				}
			}
			result.append(AMP).append(VDB_NAME_KEY).append(EQUALS).append(vdbName);
			if (hasVDBVersion) {
				result.append(AMP).append(VDB_VERSION_KEY).append(EQUALS).append(vdbVersion);
			}

		} catch (UnsupportedEncodingException err) {
			// ignore
		}

		return result.toString();
	}

	/**
	 * Returns the formatted wsdl url for the SqlQueryWebService
	 * 
	 * @param server - server name
	 * @param appContext the context of this application to use in the WSDL url
	 * @param secure - secure ssl (true) or non-secure (false)
	 * @return wsdlUrl - String
	 * @since 4.3
	 */
	public static String getSqlQueryWebServiceUrl( final String server,
												   String appContext,
	                                               final boolean secure ) {

		appContext=appContext.replace("/",""); //$NON-NLS-1$ //$NON-NLS-2$
		return MessageFormat.format(SQLQUERYWEBSERVICE_URL_FORMAT, new Object[] {secure ? HTTPS : HTTP, server,
		    secure ? getHttpsPort() : getHttpPort(), appContext+SQLQUERYWEBSERVICE_WSDL_PATH});
	}

	public static final String getHttpsPort() {
		return System.getProperty(HTTPS_PORT_PROPERTY_KEY, DEFAULT_HTTPS_PORT);
	}

	public static final String getHttpPort() {
		return System.getProperty(HTTP_PORT_PROPERTY_KEY, DEFAULT_HTTP_PORT);
	}
}
