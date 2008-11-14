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

/*
 * Date: Sep 16, 2003
 * Time: 11:23:00 AM
 */
package com.metamatrix.dqp.internal.datamgr.impl;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.data.api.ConnectorLogger;
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
}
