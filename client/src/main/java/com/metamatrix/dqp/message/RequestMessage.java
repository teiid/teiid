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

package com.metamatrix.dqp.message;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.comm.CommonCommPlugin;
import com.metamatrix.jdbc.api.ExecutionProperties;

/**
 * Request Message, used by MMXStatement for submitting queries.
 */
public class RequestMessage implements Serializable {

    static final long serialVersionUID = 2258063872049251854L;
    
    public static final int DEFAULT_FETCH_SIZE = 2000;

    private String[] commands;
    private boolean isBatchedUpdate;
    private int fetchSize = DEFAULT_FETCH_SIZE;
    private int cursorType;
    private boolean partialResultsFlag;
    private boolean isPreparedStatement;
    private boolean isCallableStatement;
    private boolean isPreparedBatchUpdate;
    private List parameterValues;
    private boolean validationMode;
    private String txnAutoWrapMode;
    private String XMLFormat;
    private String styleSheet;
    private Boolean requireResultSet;

    /**The time when the command was created by the client.*/
    private Date submittedTimestamp;

    /**The time when command begins processing on the server.*/
    private Date processingTimestamp;
    
    //whether to use ResultSet cache if there is one
    private boolean useResultSetCache;
        
    // Treat the double quoted strings as variables in the command
    private boolean dblQuotedVariableAllowed = false;
    
    //whether query plan is allowed or not
    private boolean queryPlanAllowed = true;
    
    private boolean showPlan = false;
    
    private int rowLimit;
    
    private Serializable executionPayload;
    
    private long executionId;
    
    public RequestMessage() {
    }

	public RequestMessage(String command) {
		this();
		setCommands(command);
	}

    public int getFetchSize() {
        return fetchSize;
    }

    public boolean supportsPartialResults() {
        return partialResultsFlag;
    }

    /**
     * @param i
     */
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    /**
     * @param partial
     */
    public void setPartialResults(boolean partial) {
        partialResultsFlag = partial;
    }

    /**
     * @return True if this request includes a prepared statement.
     */
    public boolean isPreparedStatement() {
        return isPreparedStatement;
    }

    /**
     * @return True if this request includes a callable statement.
     */
    public boolean isCallableStatement() {
        return isCallableStatement;
    }

    /**
     * @param isPreparedStatement
     */
    public void setPreparedStatement(boolean isPreparedStatement) {
        this.isPreparedStatement = isPreparedStatement;
    }

    /**
     * @param isCallableStatement
     */
    public void setCallableStatement(boolean isCallableStatement) {
        this.isCallableStatement = isCallableStatement;
    }

    /**
     * @return A list of parameter values. May be null.
     */
    public List getParameterValues() {
    	if (parameterValues == null) {
    		return Collections.EMPTY_LIST;
    	}
        return parameterValues;
    }

    /**
     * @param values
     */
    public void setParameterValues(List values) {
        parameterValues = values;
    }

    /**
     * @return String
     */
    public int getCursorType() {
        return cursorType;
    }

    /**
     * Sets the cursorType.
     * @param cursorType The cursorType to set
     */
    public void setCursorType(int cursorType) {
        this.cursorType = cursorType;
    }

    /**
     * @return boolean
     */
    public boolean getValidationMode() {
        return validationMode;
    }

    /**
     * @return String
     */
    public String getXMLFormat() {
        return XMLFormat;
    }

    /**
     * Sets the validationMode.
     * @param validationMode The validationMode to set
     */
    public void setValidationMode(boolean validationMode) {
        this.validationMode = validationMode;
    }

    /**
     * Sets the xMLFormat.
     * @param xMLFormat The xMLFormat to set
     */
    public void setXMLFormat(String xMLFormat) {
        XMLFormat = xMLFormat;
    }

    /**
     * @return String
     */
    public String getTxnAutoWrapMode() {
    	if (txnAutoWrapMode == null) {
    		return ExecutionProperties.AUTO_WRAP_OPTIMISTIC;
    	}
        return txnAutoWrapMode;
    }

    /**
     * Sets the txnAutoWrapMode.
     * @param txnAutoWrapMode The txnAutoWrapMode to set
     * @throws MetaMatrixProcessingException 
     */
    public void setTxnAutoWrapMode(String txnAutoWrapMode) throws MetaMatrixProcessingException {
    	if (txnAutoWrapMode != null) {
    		txnAutoWrapMode = txnAutoWrapMode.toUpperCase();
    		if (!(txnAutoWrapMode.equals(ExecutionProperties.AUTO_WRAP_OFF)
    			|| txnAutoWrapMode.equals(ExecutionProperties.AUTO_WRAP_ON)
    			|| txnAutoWrapMode.equals(ExecutionProperties.AUTO_WRAP_OPTIMISTIC)
    			|| txnAutoWrapMode.equals(ExecutionProperties.AUTO_WRAP_PESSIMISTIC))) {
    			throw new MetaMatrixProcessingException(CommonCommPlugin.Util.getString("RequestMessage.invalid_txnAutoWrap", txnAutoWrapMode)); //$NON-NLS-1$
    		}
    	} 
        this.txnAutoWrapMode = txnAutoWrapMode;
    }

    /**
     * @return String
     */
    public String getStyleSheet() {
        return styleSheet;
    }

    /**
     * Sets the styleSheet.
     * @param styleSheet The styleSheet to set
     */
    public void setStyleSheet(String styleSheet) {
        this.styleSheet = styleSheet;
    }

    /**
     * Get time that the time when the command was created by the client.
     * @return timestamp in millis
     */
    public Date getSubmittedTimestamp() {
        return submittedTimestamp;
    }
    
    /**
     * Set time that the time when the command was created by the client.
     * NOTE: By default, this gets set to the current time by the constructor.
     * @param submittedTimestamp Time submitted to server.
     */
    public void setSubmittedTimestamp(Date submittedTimestamp) {
        this.submittedTimestamp = submittedTimestamp;
    }    
    
    /**
     * Start the clock on submission start - this should be called when the request is originally created.
     */
    public void markSubmissionStart() {
        setSubmittedTimestamp(new Date());
    }
    
    
    /**
     * Get time that the request was assigned a unique ID by the server.
     * @return timestamp in millis
     */
    public Date getProcessingTimestamp() {
        return processingTimestamp;
    }

    /**
     * Set time that the request is submitted on the server.
     * @param processingTimestamp Time submitted to server.
     */
    public void setProcessingTimestamp(Date processingTimestamp) {
        this.processingTimestamp = processingTimestamp;
    }

    /**
     * Start the clock on processing times - this should be called when the query
     * hits the QueryService or SubscriptionService.
     */
    public void markProcessingStart() {
        setProcessingTimestamp(new Date());
    }

	public boolean useResultSetCache() {
		//not use caching when there is a txn 
		return useResultSetCache;
	}

	public void setUseResultSetCache(boolean useResultSetCacse) {
		this.useResultSetCache = useResultSetCacse;
	}

	public String getCommandString() {
		if (commands.length == 1) {
			return commands[0];
		}
		return Arrays.deepToString(commands);
	}
           
    public void setDoubleQuotedVariableAllowed(boolean allowed) {
        dblQuotedVariableAllowed = allowed;
    }

    public boolean isDoubleQuotedVariableAllowed() {
        return dblQuotedVariableAllowed;
    }

    public void setQueryPlanAllowed(boolean allowed) {
    	queryPlanAllowed = allowed;
    }

    public boolean isQueryPlanAllowed() {
        return queryPlanAllowed;
    }
    
    /** 
     * @return Returns the showPlan.
     * @since 4.3
     */
    public boolean getShowPlan() {
        return this.showPlan;
    }

    
    /** 
     * @param showPlan The showPlan to set.
     * @since 4.3
     */
    public void setShowPlan(boolean showPlan) {
        this.showPlan = showPlan;
    }

    
    /** 
     * @return Returns the rowLimit.
     * @since 4.3
     */
    public int getRowLimit() {
        return this.rowLimit;
    }

    
    /** 
     * @param rowLimit The rowLimit to set.
     * @since 4.3
     */
    public void setRowLimit(int rowLimit) {
        this.rowLimit = rowLimit;
    }

	public String[] getCommands() {
		return commands;
	}

	public void setCommands(String... batchedCommands) {
		this.commands = batchedCommands;
	}

	public boolean isPreparedBatchUpdate() {
		return isPreparedBatchUpdate;
	}

	public void setPreparedBatchUpdate(boolean isPreparedBatchUpdate) {
		this.isPreparedBatchUpdate = isPreparedBatchUpdate;
	}

	public void setExecutionPayload(Serializable executionPayload) {
		this.executionPayload = executionPayload;
	}

	public Serializable getExecutionPayload() {
		return executionPayload;
	}

	public long getExecutionId() {
		return executionId;
	}

	public void setExecutionId(long executionId) {
		this.executionId = executionId;
	}

	public void setBatchedUpdate(boolean isBatchedUpdate) {
		this.isBatchedUpdate = isBatchedUpdate;
	}

	public boolean isBatchedUpdate() {
		return isBatchedUpdate;
	}
	
	public Boolean getRequireResultSet() {
		return requireResultSet;
	}
	
	public void setRequireResultSet(Boolean requireResultSet) {
		this.requireResultSet = requireResultSet;
	}

}
