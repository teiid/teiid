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

package com.metamatrix.soap.sqlquerywebservice.log;

import com.metamatrix.core.log.Logger;
import com.metamatrix.internal.core.log.PlatformLog;
import com.metamatrix.soap.SOAPPlugin;
import com.metamatrix.soap.log.MMSOAPLog;

/**
 * Logger class (Singleton). It is initialized in the main servlet of the application. The Logger file name is loaded from the
 * web.xml file in ModuleServlet.
 */
public class SqlQueryWebServicePlatformLog {

	// =========================================================================
	// Static Members
	// =========================================================================
	private static final SqlQueryWebServicePlatformLog INSTANCE = new SqlQueryWebServicePlatformLog();
	PlatformLog platformLog = new PlatformLog("MetaMatrix SOAP log"); //$NON-NLS-1$

	Logger logFile = new MMSOAPLog(SOAPPlugin.PLUGIN_ID, platformLog);

	/**
	 * Private constructor
	 */
	private SqlQueryWebServicePlatformLog() {
	}

	/**
	 * Get an instance of this class.
	 */
	public static SqlQueryWebServicePlatformLog getInstance() {
		return INSTANCE;
	}

	/**
	 * Get the platform log.
	 * 
	 * @return PlatformLog
	 */
	public PlatformLog getPlatformLog() {
		return platformLog;
	}

	/**
	 * Set the platform log.
	 * 
	 * @param log PlatformLog
	 */
	public void setPlatformLog( PlatformLog log ) {
		platformLog = log;
	}

	/**
	 * Get the Logger.
	 * 
	 * @return Logger
	 */
	public Logger getLogFile() {
		return logFile;
	}

	/**
	 * Set the Logger.
	 * 
	 * @param logger
	 */
	public void setLogFile( Logger logger ) {
		logFile = logger;
	}
}
