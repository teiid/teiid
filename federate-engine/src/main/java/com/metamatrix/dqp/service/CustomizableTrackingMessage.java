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

package com.metamatrix.dqp.service;

import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.dqp.spi.CommandLoggerSPI;



class CustomizableTrackingMessage implements Runnable {
    
    static final int USER_COMMAND_START = 0;
    static final int USER_COMMAND_END = 1;
    static final int DATASOURCE_COMMAND_START = 2;
    static final int DATASOURCE_COMMAND_END = 3;
    static final int TRANSACTION_START = 4;
    static final int TRANSACTION_END = 5;
    
    int type;
    long timestamp;
    
    // Transaction info
    String transactionID;
    String subTransactionID;
    boolean isCommit;
    
    // Session info
    String sessionID;
    String applicationName;
    String principal;
    String vdbName;
    String vdbVersion;
    
    // RequestInfo
    String requestID;
    long sourceCommandID;
    String sql;
    int rowCount;
    String modelName;
    String connectorBindingName;
    boolean isCancelled;
    boolean errorOccurred;
    ExecutionContext executionContext;
    
    CommandLoggerSPI commandLogger;
    
    CustomizableTrackingMessage(long timestamp,
                                String requestID,
                                String transactionID,
                                String sessionID,
                                String applicationName,
                                String principal,
                                String vdbName,
                                String vdbVersion,
                                String sql) {
        // userCommandStart
        this.type = USER_COMMAND_START;
        this.timestamp = timestamp;
        this.requestID = requestID;
        this.transactionID = transactionID;
        this.sessionID = sessionID;
        this.applicationName = applicationName;
        this.principal = principal;
        this.vdbName = vdbName;
        this.vdbVersion = vdbVersion;
        this.sql = sql;
    }
    CustomizableTrackingMessage(long timestamp,
                                String requestID,
                                String transactionID,
                                String sessionID,
                                String principal,
                                String vdbName,
                                String vdbVersion, 
                                int finalRowCount,
                                boolean isCancelled,
                                boolean errorOccurred) {
        // userCommandEnd
        this.type = USER_COMMAND_END;
        this.timestamp = timestamp;
        this.requestID = requestID;
        this.transactionID = transactionID;
        this.sessionID = sessionID;
        this.principal = principal;
        this.vdbName = vdbName;
        this.vdbVersion = vdbVersion;
        this.rowCount = finalRowCount;
        this.isCancelled = isCancelled;
        this.errorOccurred = errorOccurred;
    }
    CustomizableTrackingMessage(long timestamp,
                                String requestID,
                                long sourceCommandID,
                                String subTransactionID,
                                String modelName, 
                                String connectorBindingName,
                                String sessionID,
                                String principal,
                                String sql,
                                ExecutionContext context) {
        // dataSourceCommandStart
        this.type = DATASOURCE_COMMAND_START;
        this.timestamp = timestamp;
        this.requestID = requestID;
        this.sourceCommandID = sourceCommandID;
        this.subTransactionID = subTransactionID;
        this.modelName = modelName;
        this.connectorBindingName = connectorBindingName;
        this.sessionID = sessionID;
        this.principal = principal;
        this.sql = sql;
        this.executionContext = context;
    }
    CustomizableTrackingMessage(long timestamp,
                                String requestID,
                                long sourceCommandID,
                                String subTransactionID,
                                String modelName, 
                                String connectorBindingName,
                                String sessionID,
                                String principal,
                                int finalRowCount, 
                                boolean isCancelled,
                                boolean errorOccurred,
                                ExecutionContext context) {
        // dataSourceCommandEnd
        this.type = DATASOURCE_COMMAND_END;
        this.timestamp = timestamp;
        this.requestID = requestID;
        this.sourceCommandID = sourceCommandID;
        this.subTransactionID = subTransactionID;
        this.modelName = modelName;
        this.connectorBindingName = connectorBindingName;
        this.sessionID = sessionID;
        this.principal = principal;
        this.rowCount = finalRowCount;
        this.isCancelled = isCancelled;
        this.errorOccurred = errorOccurred;
        this.executionContext = context;
    }
    CustomizableTrackingMessage(long timestamp,
                                String transactionID,
                                String sessionID,
                                String principal,
                                String vdbName,
                                String vdbVersion) {
        // transactionStart
        this.type = TRANSACTION_START;
        this.timestamp = timestamp;
        this.transactionID = transactionID;
        this.sessionID = sessionID;
        this.principal = principal;
        this.vdbName = vdbName;
        this.vdbVersion = vdbVersion;
    }
    CustomizableTrackingMessage(long timestamp,
                                String transactionID,
                                String sessionID,
                                String principal,
                                String vdbName,
                                String vdbVersion,
                                boolean isCommit) {
        // transactionEnd
        this.type = TRANSACTION_END;
        this.timestamp = timestamp;
        this.transactionID = transactionID;
        this.sessionID = sessionID;
        this.principal = principal;
        this.vdbName = vdbName;
        this.vdbVersion = vdbVersion;
        this.isCommit = isCommit;
    }
    
	public void run() {
		switch(type) {
        case CustomizableTrackingMessage.USER_COMMAND_START:
            commandLogger.userCommandStart(timestamp,
                                           requestID,
                                           transactionID,
                                           sessionID,
                                           applicationName,
                                           principal,
                                           vdbName,
                                           vdbVersion,
                                           sql);
            break;
        case CustomizableTrackingMessage.USER_COMMAND_END:
            commandLogger.userCommandEnd(timestamp,
                                         requestID,
                                         transactionID,
                                         sessionID,
                                         principal,
                                         vdbName,
                                         vdbVersion,
                                         rowCount,
                                         isCancelled,
                                         errorOccurred);
            break;
        case CustomizableTrackingMessage.DATASOURCE_COMMAND_START:
            commandLogger.dataSourceCommandStart(timestamp,
                                                 requestID,
                                                 sourceCommandID,
                                                 subTransactionID,
                                                 modelName,
                                                 connectorBindingName,
                                                 sessionID,
                                                 principal,
                                                 sql,
                                                 executionContext);
                                                 
            break;
        case CustomizableTrackingMessage.DATASOURCE_COMMAND_END:
            commandLogger.dataSourceCommandEnd(timestamp,
                                               requestID,
                                               sourceCommandID,
                                               subTransactionID,
                                               modelName,
                                               connectorBindingName,
                                               sessionID,
                                               principal,
                                               rowCount,
                                               isCancelled,
                                               errorOccurred,
                                               executionContext);
            break;
		}
	}

}
