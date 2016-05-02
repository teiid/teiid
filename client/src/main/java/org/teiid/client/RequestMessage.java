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

package org.teiid.client;

import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OptionalDataException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.ExternalizeUtil;
import org.teiid.jdbc.JDBCPlugin;


/**
 * Request Message, used by MMXStatement for submitting queries.
 */
public class RequestMessage implements Externalizable {

    public static final int DEFAULT_FETCH_SIZE = 2048;
    
    /** Transaction auto wrap constant - never wrap a command execution in a transaction */
    public static final String TXN_WRAP_OFF = "OFF"; //$NON-NLS-1$

    /** Transaction auto wrap constant - always wrap commands in a transaction. */
    public static final String TXN_WRAP_ON = "ON"; //$NON-NLS-1$

    /**
     * Transaction auto wrap constant - checks if a command
     * requires a transaction and will be automatically wrap it.
     */
    public static final String TXN_WRAP_DETECT = "DETECT"; //$NON-NLS-1$

    public enum StatementType {
    	PREPARED, CALLABLE, STATEMENT
    }
    
    public enum ResultsMode {
    	RESULTSET, UPDATECOUNT, EITHER
    }
    
    public enum ShowPlan {
    	ON, DEBUG, OFF
    }

    private String[] commands;
    private boolean isBatchedUpdate;
    private int fetchSize = DEFAULT_FETCH_SIZE;
    private int cursorType;
    private boolean partialResultsFlag;
    private StatementType statementType = StatementType.STATEMENT;
    private List<?> parameterValues;
    private boolean validationMode;
    private String txnAutoWrapMode;
    private String XMLFormat;
    private String styleSheet;
    private ResultsMode resultsMode = ResultsMode.EITHER;
    //whether to use ResultSet cache if there is one
    private boolean useResultSetCache;
    // Treat the double quoted strings as variables in the command
    private boolean ansiQuotedIdentifiers = true;
    private ShowPlan showPlan = ShowPlan.OFF;
    private int rowLimit;
    private Serializable executionPayload;
    private long executionId;
    private int transactionIsolation;
    private boolean noExec;
    private transient boolean sync;

	private boolean delaySerialization;
    
    public RequestMessage() {
    }

	public RequestMessage(String command) {
		this();
		setCommands(command);
	}
	
	public boolean isSync() {
		return sync;
	}
	
	public void setSync(boolean sync) {
		this.sync = sync;
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
        return this.statementType == StatementType.PREPARED;
    }

    /**
     * @return True if this request includes a callable statement.
     */
    public boolean isCallableStatement() {
        return this.statementType == StatementType.CALLABLE;
    }
    
    public void setStatementType(StatementType statementType) {
		this.statementType = statementType;
	}
	
	/**
     * @return A list of parameter values. May be null.
     */
    public List<?> getParameterValues() {
    	if (parameterValues == null) {
    		return Collections.EMPTY_LIST;
    	}
        return parameterValues;
    }

    /**
     * @param values
     */
    public void setParameterValues(List<?> values) {
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
    		return TXN_WRAP_DETECT;
    	}
        return txnAutoWrapMode;
    }

    /**
     * Sets the txnAutoWrapMode.
     * @param txnAutoWrapMode The txnAutoWrapMode to set
     * @throws TeiidProcessingException 
     */
    public void setTxnAutoWrapMode(String txnAutoWrapMode) throws TeiidProcessingException {
    	if (txnAutoWrapMode != null) {
    		txnAutoWrapMode = txnAutoWrapMode.toUpperCase();
    		if (!(txnAutoWrapMode.equals(TXN_WRAP_OFF)
    			|| txnAutoWrapMode.equals(TXN_WRAP_ON)
    			|| txnAutoWrapMode.equals(TXN_WRAP_DETECT))) {
    			throw new TeiidProcessingException(JDBCPlugin.Util.getString("RequestMessage.invalid_txnAutoWrap", txnAutoWrapMode)); //$NON-NLS-1$
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

	public boolean useResultSetCache() {
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
           
	public boolean isAnsiQuotedIdentifiers() {
		return ansiQuotedIdentifiers;
	}
	
	public void setAnsiQuotedIdentifiers(boolean ansiQuotedIdentifiers) {
		this.ansiQuotedIdentifiers = ansiQuotedIdentifiers;
	}
	
	public ShowPlan getShowPlan() {
		return showPlan;
	}
	
	public void setShowPlan(ShowPlan showPlan) {
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

	public ResultsMode getResultsMode() {
		return resultsMode;
	}
	
	public void setResultsMode(ResultsMode resultsMode) {
		this.resultsMode = resultsMode;
	}
	
	public int getTransactionIsolation() {
		return transactionIsolation;
	}
	
	public void setTransactionIsolation(int transactionIsolation) {
		this.transactionIsolation = transactionIsolation;
	}
	
	public boolean isNoExec() {
		return noExec;
	}
	
	public void setNoExec(boolean noExec) {
		this.noExec = noExec;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.commands = ExternalizeUtil.readStringArray(in);
		this.isBatchedUpdate = in.readBoolean();
		this.fetchSize = in.readInt();
		this.cursorType = in.readInt();
		this.partialResultsFlag = in.readBoolean();
		this.statementType = StatementType.values()[in.readByte()];
		this.parameterValues = ExternalizeUtil.readList(in);
		this.validationMode = in.readBoolean();
		this.txnAutoWrapMode = (String)in.readObject();
		this.XMLFormat = (String)in.readObject();
		this.styleSheet = (String)in.readObject();
		this.resultsMode = ResultsMode.values()[in.readByte()];
		this.useResultSetCache = in.readBoolean();
		this.ansiQuotedIdentifiers = in.readBoolean();
		this.showPlan = ShowPlan.values()[in.readByte()];
		this.rowLimit = in.readInt();
		this.executionPayload = (Serializable)in.readObject();
		this.executionId = in.readLong();
		this.transactionIsolation = in.readInt();
		this.noExec = in.readBoolean();
 		try {
			byte options = in.readByte();
			//8.4 property
			this.delaySerialization = (options & 2) == 2;
 		} catch (OptionalDataException e) {
 		} catch (EOFException e) {
 		}
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		ExternalizeUtil.writeArray(out, commands);
		out.writeBoolean(isBatchedUpdate);
		out.writeInt(fetchSize);
		out.writeInt(cursorType);
		out.writeBoolean(partialResultsFlag);
		out.writeByte(statementType.ordinal());
		ExternalizeUtil.writeList(out, parameterValues);
		out.writeBoolean(validationMode);
		out.writeObject(txnAutoWrapMode);
		out.writeObject(XMLFormat);
		out.writeObject(styleSheet);
		out.writeByte(resultsMode.ordinal());
		out.writeBoolean(useResultSetCache);
		out.writeBoolean(ansiQuotedIdentifiers);
		out.writeByte(showPlan.ordinal());
		out.writeInt(rowLimit);
		out.writeObject(executionPayload);
		out.writeLong(executionId);
		out.writeInt(transactionIsolation);
		out.writeBoolean(noExec);
		byte options = 0;
		if (delaySerialization) {
			options |= 2;
		}
		out.writeByte(options);
	}
	
	public boolean isDelaySerialization() {
		return delaySerialization;
	}
	
	public void setDelaySerialization(boolean delaySerialization) {
		this.delaySerialization = delaySerialization;
	}
	
}
