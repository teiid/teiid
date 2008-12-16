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

package com.metamatrix.soap.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import javax.servlet.http.HttpServletRequest;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.ws.security.WSConstants;
import org.apache.ws.security.WSSecurityEngineResult;
import org.apache.ws.security.WSUsernameTokenPrincipal;
import org.apache.ws.security.handler.WSHandlerConstants;
import org.apache.ws.security.handler.WSHandlerResult;
import com.metamatrix.admin.api.core.Admin;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.objects.VDB;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.comm.platform.client.ServerAdminFactory;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.soap.SOAPPlugin;
import com.metamatrix.soap.exceptions.SOAPProcessingException;
import com.metamatrix.soap.object.MMServerInfo;
import com.metamatrix.soap.object.WSDLUrl;
import com.metamatrix.soap.security.Credential;

/**
 * This class contains utility methods for MetaMatrix Web Services.
 */
public class WebServiceUtil {
	
	/*
	 * This is the username and password for the WSDL user account. This account is strictly used
	 * for getting VDB resources (like the WSDL and schemas). There is a check in place in the Request
	 * to assure that the getUpdatedVDBResources is the only procedure used by this account. The password 
	 * is not checked so it's value is not important, it just can't be blank.
	 */
	public static final String WSDLUSER = "MetaMatrixWSDLUser"; //$NON-NLS-1$
	public static final String WSDLPASSWORD = "mmx"; //$NON-NLS-1$

	/**
	 * This method will validate that the action value is set. If the value is not set a SOAPException will be thrown. The value
	 * should contain the fully qualified procedure name. For 5.5 WSDL and greater the value should also contain the server
	 * connection properties in order to support non-HTTP transports (like JMS).
	 * 
	 * @param action - Action string
	 * @return boolean (true if set)
	 * @exception SOAPProcessingException (if value not set)
	 */
	public static void validateActionIsSet( String action ) throws SOAPProcessingException {

		if (action == null || action.trim().equals(StringUtil.Constants.EMPTY_STRING)) {
			throw new SOAPProcessingException(SOAPPlugin.Util.getString("WebServiceUtil.0")); //$NON-NLS-1$
		}
	}

	/**
	 * This method gets the user credentials containg username and password to use for a server connection. It will return a
	 * WSUsernameTokenPrincipal if WS-Security was used and a string containing encrypted username and password if HTTPBasic was
	 * used. If neither was used an exception will be thrown.
	 * 
	 * @return Object (WSUsernameTokenPrincipal or String)
	 * @throws AxisFault
	 * @since 5.5
	 */
	public static Credential getCredentials( final MessageContext msgCtx ) throws AxisFault {

		WSUsernameTokenPrincipal principal = null;
		String basicAuthenticationString = null;

		/*
		 * These are thread contextual calls that we can make in this web service implementation to get the authorization
		 * information for the current call.
		 */

		Vector results = null;

		if ((results = (Vector)msgCtx.getProperty(WSHandlerConstants.RECV_RESULTS)) != null) {
			for (int i = 0; i < results.size(); i++) {
				WSHandlerResult rResult = (WSHandlerResult)results.get(i);
				Vector wsSecEngineResults = rResult.getResults();

				for (int j = 0; j < wsSecEngineResults.size(); j++) {
					WSSecurityEngineResult wser = (WSSecurityEngineResult)wsSecEngineResults.get(j);
					if (((java.lang.Integer)wser.get(WSSecurityEngineResult.TAG_ACTION)).intValue() == WSConstants.UT
					    && wser.get(WSSecurityEngineResult.TAG_PRINCIPAL) != null) {

						// Extract the principal
						principal = (WSUsernameTokenPrincipal)wser.get(WSSecurityEngineResult.TAG_PRINCIPAL);
					}
				}
			}
		}

		// If WS-Security token was not found, check for HTTPBasic
		if (principal == null) {
			HttpServletRequest req = (HttpServletRequest)msgCtx.getProperty(HTTPConstants.MC_HTTP_SERVLETREQUEST);

			// Acquiring Authorization Header from servlet request
			basicAuthenticationString = req.getHeader("Authorization"); //$NON-NLS-1$

			if (basicAuthenticationString != null) {
				basicAuthenticationString = basicAuthenticationString.substring(basicAuthenticationString.indexOf(" ")); //$NON-NLS-1$
			}
		}

		if (principal == null && basicAuthenticationString == null) {
			throw (new AxisFault(SOAPPlugin.Util.getString("WebServiceUtil.1"))); //$NON-NLS-1$
		}

		Credential credential = null;
		if (principal != null) {
			credential = new Credential(principal.getName(), principal.getPassword().getBytes());
		} else {
			// Assume Basic
			credential = getBasicUserNameAndPassword(basicAuthenticationString);
		}

		return credential;
	}

	/**
	 * This decodes and parses the username and password from an encrypted string derived from HTTPBasic and returns in a <@link
	 * come.metamatrix.soap.security.Credential> instance.
	 * 
	 * @return userName - String
	 */
	public static Credential getBasicUserNameAndPassword( final Object creds ) {

		// Decoding the authorization header...
		String decoded = new String(org.apache.axiom.om.util.Base64.decode(creds.toString()));

		// decoded now contains username:password in plain text.
		int i = decoded.indexOf(":"); //$NON-NLS-1$

		// so we take the username from it ( everything until the ':' )
		String userName = decoded.substring(0, i);

		// and the password
		String password = decoded.substring(i + 1, decoded.length());

		return new Credential(userName, password.getBytes());
	}

	/**
	 * Returns a list of WSDLUrl objects that define WSDL for MetaMatrix Web Service VDBs.
	 * 
	 * @param webScheme
	 * @param webHost
	 * @param webPort
	 * @param appName
	 * @param userName
	 * @param password
	 * @param serverList
	 * @return List list of WSDLUrl objects that define the MetaMatrix Web Service VDB WSDL
	 * @throws SQLException
	 * @throws LogonException
	 * @throws AdminException
	 * @since 5.5.3
	 */
	public static List getWSDLUrls( final String webScheme,
	                                final String webHost,
	                                final String webPort,
	                                final String appName,
	                                final String userName,
	                                final String password,
	                                final MMServerInfo serverList ) throws LogonException, AdminException {
		List<WSDLUrl> wsdlUrlList = new LinkedList<WSDLUrl>();

		Admin serverAdmin = getAdminAPI(userName, password, serverList);

		Collection vdbCollection = serverAdmin.getVDBs("*"); //$NON-NLS-1$

		Iterator iterVdbs = vdbCollection.iterator();

		while (iterVdbs.hasNext()) {
			VDB vdb = (VDB)iterVdbs.next();
			if (vdb.getState() == VDB.ACTIVE && vdb.hasWSDL()) {
				wsdlUrlList.add(new WSDLUrl(webHost, webPort, appName, webScheme, vdb.getName(), vdb.getVDBVersion()));
			}
		}

		return wsdlUrlList;
	}

	/**
	 * @param userName
	 * @param password
	 * @param serverUrlInfo
	 * @return Admin
	 * @throws LogonException, AdminException
	 * @since 5.5.3
	 */
	public static Admin getAdminAPI( final String userName,
	                                 final String password,
	                                 final MMServerInfo serverUrlInfo ) throws AdminException {
		Admin serverAdmin = null;

		com.metamatrix.common.comm.platform.client.ServerAdminFactory factory = ServerAdminFactory.getInstance();
		serverAdmin = factory.createAdmin(userName, password.toCharArray(), serverUrlInfo.toString());

		return serverAdmin;
	}
}
