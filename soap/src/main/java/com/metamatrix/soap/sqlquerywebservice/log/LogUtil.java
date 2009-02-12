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

package com.metamatrix.soap.sqlquerywebservice.log;

import com.metamatrix.core.log.LogMessage;
import com.metamatrix.internal.core.log.PlatformLog;
import com.metamatrix.soap.SOAPPlugin;

/**
 * This is going to console, if needs to be file based, a log listener needs to be added to this.
 */
public class LogUtil {

	// =========================================================================
	// Static Members
	// =========================================================================
	private static final LogUtil INSTANCE = new LogUtil();

	PlatformLog log = new PlatformLog("MetaMatrix SOAP log"); //$NON-NLS-1$

	/**
	 * Get an instance of this class.
	 */
	public static LogUtil getInstance() {
		return INSTANCE;
	}
	
	public static void log(int severity, String message) {
		INSTANCE.log.logMessage(new LogMessage(SOAPPlugin.PLUGIN_ID, severity, new Object[] {message}));
	}

	public static void log(int severity, Throwable t, String message) {
		INSTANCE.log.logMessage(new LogMessage(SOAPPlugin.PLUGIN_ID, severity, t, new Object[] {message}));
	}	
}
