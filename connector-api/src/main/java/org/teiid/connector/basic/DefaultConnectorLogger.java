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

/*
 * Date: Sep 16, 2003
 * Time: 11:23:00 AM
 */
package org.teiid.connector.basic;

import org.teiid.connector.api.ConnectorLogger;

/**
 * DefaultConnectorLogger.
 */
public class DefaultConnectorLogger implements ConnectorLogger {
	public static final String CTX_CONNECTOR = "CONNECTOR"; //$NON-NLS-1$
	@Override
    public void logError(String message) {
        //LogManager.logError(CTX_CONNECTOR, message);
    }
    @Override
    public void logError(String message, Throwable error) {
       // LogManager.logError(CTX_CONNECTOR, error, message);
    }
    @Override
    public void logWarning(String message) {
       // LogManager.logWarning(CTX_CONNECTOR, message);
    }
    @Override
    public void logInfo(String message) {
        //LogManager.logInfo(CTX_CONNECTOR, message);
    }
    @Override
    public void logDetail(String message) {
        //LogManager.logDetail(CTX_CONNECTOR, message);
    }

    @Override
    public void logTrace(String message) {
       // LogManager.logTrace(CTX_CONNECTOR, message);
    }

	@Override
	public boolean isDetailEnabled() {
		//return LogManager.isMessageToBeRecorded(CTX_CONNECTOR, MessageLevel.DETAIL);
		return true;
	}

	@Override
	public boolean isErrorEnabled() {
		//return LogManager.isMessageToBeRecorded(CTX_CONNECTOR, MessageLevel.ERROR);
		return true;
	}

	@Override
	public boolean isInfoEnabled() {
		//return LogManager.isMessageToBeRecorded(CTX_CONNECTOR, MessageLevel.INFO);
		return true;
	}

	@Override
	public boolean isTraceEnabled() {
		//return LogManager.isMessageToBeRecorded(CTX_CONNECTOR, MessageLevel.TRACE);
		return true;
	}

	@Override
	public boolean isWarningEnabled() {
		//return LogManager.isMessageToBeRecorded(CTX_CONNECTOR, MessageLevel.WARNING);
		return true;
	}

	@Override
	public void logDetail(String message, Throwable error) {
		//LogManager.log(MessageLevel.DETAIL, CTX_CONNECTOR, error, message);
	}

	@Override
	public void logInfo(String message, Throwable error) {
		//LogManager.log(MessageLevel.INFO, CTX_CONNECTOR, error, message);
	}

	@Override
	public void logTrace(String message, Throwable error) {
		//LogManager.log(MessageLevel.TRACE, CTX_CONNECTOR, error, message);
	}

	@Override
	public void logWarning(String message, Throwable error) {
		//LogManager.log(MessageLevel.WARNING, CTX_CONNECTOR, error, message);
	}
}
