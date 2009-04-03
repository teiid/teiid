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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.metamatrix.api.exception.ExceptionHolder;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.batch.BatchSerializer;
import com.metamatrix.core.util.ExternalizeUtil;

/**
 * Results Message, used by MMStatement to get the query results.
 */
public class ResultsMessage implements Externalizable {

    static final long serialVersionUID = 3546924172976187793L;

	private List[] results = null;
	private String[] columnNames = null;
	private String[] dataTypes = null;

    /** A description of planning that occurred as requested in the request. */
    private Map planDescription;

    /** An exception that occurred. */
    private MetaMatrixException exception;

    /** Warning could be schema validation errors or partial results warnings */
    private List<Throwable> warnings;

    /** Schemas associated with xml results. */
    private Collection schemas;

    /** First row index */
    private int firstRow = 0;

    /** Last row index */
    private int lastRow;

    /** Flag indicating whether this result set is part or all of the entire result set */
    private boolean partialResultsFlag;

    /** Final row index in complete result set, if known */
    private int finalRow = -1;

    /** The parameters of a Stored Procedure */
    private List parameters;

    /** This object represents the time when command is submitted to the server. */
    private Date processingTimestamp;

    /** This object represents the time when results are produced on the server. */
    private Date completedTimestamp;

    /** Fetch size for the results, if appropriate */
    private int fetchSize;

    /** Cursor type for the results, if appropriate */
    private int cursorType;

    /** OPTION DEBUG log if OPTION DEBUG was used */
    private String debugLog;
        
    /** 
     * Query plan annotations, if OPTION SHOWPLAN or OPTION PLANONLY was used:
     * Collection of Object[] where each Object[] holds annotation information
     * that can be used to create an Annotation implementation in JDBC.  
     */
    private Collection annotations;
    
    private boolean isUpdateResult;

    public ResultsMessage(){
    }

    /**
     * Instantiate and copy relevant information from the original request message.
     * Typically, the transaction context should only be copied if this results
     * message is being returned from the connector to the query engine. Clients
     * will be unable to deserialize this object.
     * @param requestMsg
     * @param copyTransactionContext true if the transaction context should be copied; false otherwise.
     * @since 4.2
     */
    public ResultsMessage(RequestMessage requestMsg){
        if(requestMsg != null){
            this.processingTimestamp = requestMsg.getProcessingTimestamp();
            this.completedTimestamp = new Date();
            this.fetchSize = requestMsg.getFetchSize();
            this.cursorType = requestMsg.getCursorType();
        }
        this.results = new ArrayList[0];

    }

    public ResultsMessage(RequestMessage requestMsg, List[] results, String[] columnNames, String[] dataTypes){
        this (requestMsg);
        setResults( results );
        setFirstRow( 1 );
        setLastRow( results.length );
        setPartialResults( false );

        this.columnNames = columnNames;
        this.dataTypes = dataTypes;
    }

	public  List[] getResults() {
		return results;
	}

    public void setResults(List[] results) {
		this.results = results;
	}

	public  String[] getColumnNames() {
        return this.columnNames;
	}

	public String[] getDataTypes() {
        return this.dataTypes;
	}

    /**
     * @return
     */
    public MetaMatrixException getException() {
        return exception;
    }

    /**
     * @return
     */
    public int getFinalRow() {
        return finalRow;
    }

    /**
     * @return
     */
    public int getFirstRow() {
        return firstRow;
    }

    /**
     * @return
     */
    public int getLastRow() {
        return lastRow;
    }

    /**
     * @return
     */
    public boolean isPartialResults() {
        return partialResultsFlag;
    }

    /**
     * @return
     */
    public Map getPlanDescription() {
        return planDescription;
    }


    /**
     * @return
     */
    public Collection getSchemas() {
        return schemas;
    }

    /**
     * @return
     */
    public List getWarnings() {
        return warnings;
    }

    /**
     * @param exception
     */
    public void setException(Throwable e) {
        if(e instanceof MetaMatrixException) {
            this.exception = (MetaMatrixException)e;
        } else {
            this.exception = new MetaMatrixException(e, e.getMessage());
        }
    }

    /**
     * @param i
     */
    public void setFinalRow(int i) {
        finalRow = i;
    }

    /**
     * @param i
     */
    public void setFirstRow(int i) {
        firstRow = i;
    }

    /**
     * @param i
     */
    public void setLastRow(int i) {
        lastRow = i;
    }

    /**
     * @param b
     */
    public void setPartialResults(boolean b) {
        partialResultsFlag = b;
    }

    /**
     * @param object
     */
    public void setPlanDescription(Map object) {
        planDescription = object;
    }

    /**
     * @param collection
     */
    public void setSchemas(Collection collection) {
        schemas = collection;
    }

    /**
     * @param list
     */
    public void setWarnings(List<Throwable> list) {
        warnings = list;
    }

    /**
     * @return
     */
    public List getParameters() {
        return parameters;
    }

    /**
     * @param list
     */
    public void setParameters(List list) {
        parameters = list;
    }

    public Date getProcessingTimestamp() {
        return this.processingTimestamp;
    }

    public Date getCompletedTimestamp() {
        return this.completedTimestamp;
    }
    /**
     * @param strings
     */
    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    /**
     * @param strings
     */
    public void setDataTypes(String[] dataTypes) {
        this.dataTypes = dataTypes;
    }

    /**
     * @return
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * @param i
     */
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    /**
     * @return
     */
    public int getCursorType() {
        return cursorType;
    }

    /**
     * @param i
     */
    public void setCursorType(int cursorType) {
        this.cursorType = cursorType;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        columnNames = ExternalizeUtil.readStringArray(in);
        dataTypes = ExternalizeUtil.readStringArray(in);

        // Row data
        results = BatchSerializer.readBatch(in, dataTypes);

        // Plan Descriptions
        planDescription = ExternalizeUtil.readMap(in);

        ExceptionHolder holder = (ExceptionHolder)in.readObject();
        if (holder != null) {
        	this.exception = (MetaMatrixException)holder.getException();
        }
        List<ExceptionHolder> holderList = (List<ExceptionHolder>)in.readObject();
        if (holderList != null) {
        	this.warnings = ExceptionHolder.toThrowables(holderList);
        }

        //Schemas
        schemas = ExternalizeUtil.readList(in);

        firstRow = in.readInt();
        lastRow = in.readInt();
        partialResultsFlag = in.readBoolean();
        finalRow = in.readInt();

        //Parameters
        parameters = ExternalizeUtil.readList(in);

        processingTimestamp = (Date)in.readObject();
        completedTimestamp = (Date)in.readObject();
        fetchSize = in.readInt();
        cursorType = in.readInt();
        debugLog = (String)in.readObject();
        annotations = (Collection)in.readObject();
        isUpdateResult = in.readBoolean();
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        ExternalizeUtil.writeArray(out, columnNames);
        ExternalizeUtil.writeArray(out, dataTypes);

        // Results data
        BatchSerializer.writeBatch(out, dataTypes, results);

        // Plan descriptions
        ExternalizeUtil.writeMap(out, planDescription);

        if (exception != null) {
        	out.writeObject(new ExceptionHolder(exception));
        } else {
        	out.writeObject(exception);
        }
        if (this.warnings != null) {
        	out.writeObject(ExceptionHolder.toExceptionHolders(this.warnings));
        } else {
        	out.writeObject(this.warnings);
        }

        //Schemas
        ExternalizeUtil.writeCollection(out, schemas);
        out.writeInt(firstRow);
        out.writeInt(lastRow);
        out.writeBoolean(partialResultsFlag);
        out.writeInt(finalRow);

        // Parameters
        ExternalizeUtil.writeList(out, parameters);

        out.writeObject(processingTimestamp);
        out.writeObject(completedTimestamp);
        out.writeInt(fetchSize);
        out.writeInt(cursorType);
        out.writeObject(debugLog);
        out.writeObject(annotations);
        out.writeBoolean(isUpdateResult);
    }

    /**
     * @return
     */
    public Collection getAnnotations() {
        return annotations;
    }

    /**
     * @return
     */
    public String getDebugLog() {
        return debugLog;
    }

    /**
     * @param collection
     */
    public void setAnnotations(Collection collection) {
        annotations = collection;
    }

    /**
     * @param string
     */
    public void setDebugLog(String string) {
        debugLog = string;
    }
    
          
    /* 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return new StringBuffer("ResultsMessage rowCount=") //$NON-NLS-1$
            .append(results == null ? 0 : results.length)
            .append(" finalRow=") //$NON-NLS-1$
            .append(finalRow)
            .toString();
    }

	public void setUpdateResult(boolean isUpdateResult) {
		this.isUpdateResult = isUpdateResult;
	}

	public boolean isUpdateResult() {
		return isUpdateResult;
	}
}

