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

package com.metamatrix.connector.xmlsource;

import java.util.ResourceBundle;

import org.teiid.connector.api.ConnectorLogger;

import com.metamatrix.core.BundleUtil;

/**
 * For logging purposes
 */
public class XMLSourcePlugin { // extends Plugin {

    /**
     * The plug-in identifier of this plugin
     */
    public static final String PLUGIN_ID = "com.metamatrix.connector.xmlsource" ; //$NON-NLS-1$

	public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,
	                                                     PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$


    
    /**
     * Log an error message.
     * @param message The message
     */
    public static void logError(ConnectorLogger logger, String message ) {
        String msg = Util.getString(message);
        logger.logError(msg);
    }

    public static void logError(ConnectorLogger logger, String message, Object[] params) {
        String msg = Util.getString(message, params);
        logger.logError(msg);
    }

    /**
     * Log an error message with an error, which may allow the stack
     * trace for the error to be logged, depending on the log configuration.
     * @param message The message
     * @param error The error
     */
    public static void logError(ConnectorLogger logger, String message, Throwable error ) {
        String msg = Util.getString(message);
        logger.logError(msg, error);
    }

    public static void logError(ConnectorLogger logger, String message, Object[] params, Throwable error) {
        String msg = Util.getString(message, params);
        logger.logError(msg, error);
    }

    /**
     * Log a warning message.
     * @param message The message
     */
    public static void logWarning(ConnectorLogger logger, String message ) {
        String msg = Util.getString(message);
        logger.logWarning(msg);
    }

    public static void logWarning(ConnectorLogger logger, String message, Object[] params) {
        String msg = Util.getString(message, params);
        logger.logWarning(msg);
    }

    /**
     * Log an informational message.
     * @param message The message
     */
    public static void logInfo(ConnectorLogger logger, String message ) {
        String msg = Util.getString(message);
        logger.logInfo(msg);
    }

    public static void logInfo(ConnectorLogger logger, String message, Object[] params) {
        String msg = Util.getString(message, params);
        logger.logInfo(msg);
    }

    /**
     * Log a detail debugging message.
     * @param message The message
     */
    public static void logDetail(ConnectorLogger logger, String message ) {
        String msg = Util.getString(message);
        logger.logDetail(msg);
    }

    public static void logDetail(ConnectorLogger logger, String message, Object[] params) {
        String msg = Util.getString(message, params);
        logger.logDetail(msg);
    }

    /**
     * Log a trace debugging message.
     * @param message The message
     */
    public static void logTrace(ConnectorLogger logger, String message ) {
        String msg = Util.getString(message);
        logger.logTrace(msg);
    }

    public static void logTrace(ConnectorLogger logger, String message, Object[] params) {
        String msg = Util.getString(message, params);
        logger.logTrace(msg);
    }

}
