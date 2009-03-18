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
package org.teiid.dqp.internal.datamgr.impl;

import org.teiid.connector.api.ConnectorLogger;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.util.LogConstants;

/**
 * DefaultConnectorLogger.
 */
public class DefaultConnectorLogger implements ConnectorLogger {
    public DefaultConnectorLogger(ConnectorID loggingID) {
    }

    public void logError(String message) {
        LogManager.logError(LogConstants.CTX_CONNECTOR, message);
    }

    public void logError(String message, Throwable error) {
        LogManager.logError(LogConstants.CTX_CONNECTOR, error, message);
    }

    public void logWarning(String message) {
        LogManager.logWarning(LogConstants.CTX_CONNECTOR, message);
    }

    public void logInfo(String message) {
        LogManager.logInfo(LogConstants.CTX_CONNECTOR, message);
    }

    public void logDetail(String message) {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, message);
    }

    public void logTrace(String message) {
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, message);
    }

	@Override
	public boolean isDetailEnabled() {
		return LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL);
	}

	@Override
	public boolean isErrorEnabled() {
		return LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.ERROR);
	}

	@Override
	public boolean isInfoEnabled() {
		return LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.INFO);
	}

	@Override
	public boolean isTraceEnabled() {
		return LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.TRACE);
	}

	@Override
	public boolean isWarningEnabled() {
		return LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.WARNING);
	}

	@Override
	public void logDetail(String message, Throwable error) {
		LogManager.log(MessageLevel.DETAIL, LogConstants.CTX_CONNECTOR, error, message);
	}

	@Override
	public void logInfo(String message, Throwable error) {
		LogManager.log(MessageLevel.INFO, LogConstants.CTX_CONNECTOR, error, message);
	}

	@Override
	public void logTrace(String message, Throwable error) {
		LogManager.log(MessageLevel.TRACE, LogConstants.CTX_CONNECTOR, error, message);		
	}

	@Override
	public void logWarning(String message, Throwable error) {
		LogManager.log(MessageLevel.WARNING, LogConstants.CTX_CONNECTOR, error, message);	
	}
}
