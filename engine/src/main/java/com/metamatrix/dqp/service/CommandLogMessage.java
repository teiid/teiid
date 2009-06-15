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

package com.metamatrix.dqp.service;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.teiid.connector.api.ExecutionContext;



public class CommandLogMessage {
    
    static final int USER_COMMAND_START = 0;
    static final int USER_COMMAND_END = 1;
    static final int DATASOURCE_COMMAND_START = 2;
    static final int DATASOURCE_COMMAND_END = 3;

	public static final short CMD_POINT_BEGIN = 1;
	public static final short CMD_POINT_END = 2;

	public static final short CMD_STATUS_NEW = 1;
	public static final short CMD_STATUS_END = 2;
	public static final short CMD_STATUS_CANCEL = 3;
	public static final short CMD_STATUS_ERROR = 4;
    
    private static final String TIMESTAMP_FORMAT = "yyyy.MM.dd HH:mm:ss.SSS"; //$NON-NLS-1$
    private static DateFormat TIMESTAMP_FORMATTER = new SimpleDateFormat(TIMESTAMP_FORMAT);
    
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
    
    
    public CommandLogMessage(long timestamp,
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
    public CommandLogMessage(long timestamp,
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
    public CommandLogMessage(long timestamp,
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
    public CommandLogMessage(long timestamp,
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
    
    public String toString() {
    	switch (this.type) {
	    	case USER_COMMAND_START:
	    		return getTimestampString(new Date()) + "\tSTART USER COMMAND:\tstartTime=" + getTimestampString(new Date(timestamp)) + "\trequestID=" + requestID + "\ttxID=" + transactionID + "\tsessionID=" + sessionID + "\tapplicationName=" + applicationName + "\tprincipal=" + principal + "\tvdbName=" + vdbName + "\tvdbVersion=" + vdbVersion + "\tsql=" + sql;  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
	    	case USER_COMMAND_END:
	    		return getTimestampString(new Date()) + "\tEND USER COMMAND:\tendTime=" + getTimestampString(new Date(timestamp)) + "\trequestID=" + requestID + "\ttxID=" + transactionID + "\tsessionID=" + sessionID + "\tprincipal=" + principal + "\tvdbName=" + vdbName + "\tvdbVersion=" + vdbVersion + "\tfinalRowCount=" + rowCount + "\tisCancelled=" + isCancelled + "\terrorOccurred=" + errorOccurred;  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$
	    	case DATASOURCE_COMMAND_START:
	    		return getTimestampString(new Date()) + "\tSTART DATA SRC COMMAND:\tstartTime=" + getTimestampString(new Date(timestamp)) + "\trequestID=" + requestID + "\tsourceCommandID="+ sourceCommandID + "\tsubTxID=" + subTransactionID + "\tmodelName="+ modelName + "\tconnectorBindingName=" + connectorBindingName + "\tsessionID=" + sessionID + "\tprincipal=" + principal + "\tsql=" + sql;  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$
	    	case DATASOURCE_COMMAND_END:
	    		return getTimestampString(new Date()) + "\tEND DATA SRC COMMAND:\tendTime=" + getTimestampString(new Date(timestamp)) + "\trequestID=" + requestID + "\tsourceCommandID="+ sourceCommandID + "\tsubTxID=" + subTransactionID + "\tmodelName="+ modelName + "\tconnectorBindingName=" + connectorBindingName + "\tsessionID=" + sessionID + "\tprincipal=" + principal + "\tfinalRowCount=" + rowCount + "\tisCancelled=" + isCancelled + "\terrorOccurred=" + errorOccurred;  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$//$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$ //$NON-NLS-11$
    	}
    	return null;
    }

    private String getTimestampString(Date date) {
        return TIMESTAMP_FORMATTER.format(date);
    }    
}
