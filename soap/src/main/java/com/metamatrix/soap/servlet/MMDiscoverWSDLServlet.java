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
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.util.WSDLServletUtil;
import com.metamatrix.core.log.FileLogWriter;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.soap.SOAPPlugin;
import com.metamatrix.soap.object.MMServerInfo;
import com.metamatrix.soap.util.WebServiceUtil;

/**
 * Servlet to retrieve WSDL from a VDB using servlet context values specified in the web.xml.
 * 
 * @since 5.5.3
 */
public class MMDiscoverWSDLServlet extends MMGetWSDLServlet {

	/** MM Server host/port/protocol */
	String mmServer = StringUtil.Constants.EMPTY_STRING;

	String mmProtocol = StringUtil.Constants.EMPTY_STRING;

	public MMDiscoverWSDLServlet() {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	synchronized public void init( ServletConfig config ) throws ServletException {
		super.init(config);

		String logFile = getServletContext().getInitParameter("logfile"); //$NON-NLS-1$
		File log = new File(logFile);
		logWriter = new FileLogWriter(log);
		platformLog.getPlatformLog().addListener(logWriter);

		mmServer = getServletContext().getInitParameter("mmServer"); //$NON-NLS-1$
		mmProtocol = getServletContext().getInitParameter("mmProtocol"); //$NON-NLS-1$        
	}

	/**
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 * @since 5.5.3
	 */
	public void doPost( HttpServletRequest req,
	                    HttpServletResponse resp ) throws ServletException, IOException {

		MMServerInfo serverInfo = new MMServerInfo(mmProtocol, mmServer);

		try {
			List wsdlUrls = WebServiceUtil.getWSDLUrls(req.getScheme(),
			                                           req.getServerName(),
			                                           Integer.toString(req.getServerPort()),
			                                           req.getContextPath(),
			                                           WebServiceUtil.WSDLUSER,
			                                           WebServiceUtil.WSDLPASSWORD,
			                                           serverInfo);
			req.getSession().setAttribute(WSDLServletUtil.DISCOVERED_WSDL, wsdlUrls);
			getServletConfig().getServletContext().getRequestDispatcher("/wsdlurls.jsp").forward(req, resp); //$NON-NLS-1$

		} catch (LogonException e) {
			MMGetVDBResourcePlatformLog.getInstance().getLogFile().log(MessageLevel.ERROR, e, e.getMessage());
			resp.getOutputStream().println(SOAPPlugin.Util.getString("MMDiscoverWSDLServlet.2") + e.getMessage()); //$NON-NLS-1$
		} catch (AdminException e) {
			MMGetVDBResourcePlatformLog.getInstance().getLogFile().log(MessageLevel.ERROR, e, e.getMessage());
			resp.getOutputStream().println(SOAPPlugin.Util.getString("MMDiscoverWSDLServlet.2") + e.getMessage()); //$NON-NLS-1$
		} catch (Exception e) {
			MMGetVDBResourcePlatformLog.getInstance().getLogFile().log(MessageLevel.ERROR, e, e.getMessage());
			resp.getOutputStream().println(SOAPPlugin.Util.getString("MMDiscoverWSDLServlet.1") + e.getMessage()); //$NON-NLS-1$
		}
	}
}
