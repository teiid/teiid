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

package com.metamatrix.dqp.message;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;
import java.util.List;

import com.metamatrix.common.batch.BatchSerializer;
import com.metamatrix.core.util.ExternalizeUtil;

public class AtomicResultsMessage implements Externalizable {

    // atomic request messages's id if this is result for a atomic request.
    private AtomicRequestID atomicRequestId;
	
    // This object represents the time when command is submitted to the server.
    private Date processingTimestamp;

    // This object represents the time when results are produced on the server.
    private Date completedTimestamp;
	
	private List[] results = null;
	private String[] columnNames = null;
	private String[] dataTypes = null;

    // First row index 
    private int firstRow = 0;

    // Last row index
    private int lastRow;	
    
    // Flag indicating whether this result set is part or all of the entire result set 
    private boolean partialResultsFlag;

    // Final row index in complete result set, if known
    private int finalRow = -1;
    
    // by default we support implicit close.
    private boolean supportsImplicitClose = true;

    // this flag is used to notify the connector state
    private boolean requestClosed = false;
    
    private boolean isTransactional;

    // to honor the externalizable contract
	public AtomicResultsMessage() {
	}
	
	public AtomicResultsMessage(AtomicRequestMessage msg, List[] results, String[] columnNames, String[] dataTypes) {

		this(msg);
		
        this.results = results;
        this.columnNames = columnNames;
        this.dataTypes = dataTypes;
        this.firstRow = 1;
        this.lastRow = results.length;
        this.partialResultsFlag = false;
	}
	
	public AtomicResultsMessage(AtomicRequestMessage msg) {
        this.processingTimestamp = msg.getProcessingTimestamp();
        this.completedTimestamp = new Date();
        this.atomicRequestId = msg.getAtomicRequestID();
	}
	
    public boolean supportsImplicitClose() {
        return this.supportsImplicitClose;
    }
    
    public void setSupportsImplicitClose(boolean supportsImplicitClose) {
        this.supportsImplicitClose = supportsImplicitClose;
    }    
    
    public int getFirstRow() {
        return firstRow;
    }
    
    public void setFirstRow(int i) {
        firstRow = i;
    }

    public int getLastRow() {
        return lastRow;
    }    
    
    public void setLastRow(int i) {
        lastRow = i;
    }
    
    public boolean isPartialResults() {
        return partialResultsFlag;
    }
    
    public void setPartialResults(boolean b) {
        partialResultsFlag = b;
    }
    
    public int getFinalRow() {
        return finalRow;
    }
    
    public void setFinalRow(int i) {
        finalRow = i;
    }

    public boolean isRequestClosed() {
        return this.requestClosed;
    }

    public void setRequestClosed(boolean requestClosed) {
        this.requestClosed = requestClosed;
    }     
    
	public AtomicRequestID getAtomicRequestId() {
		return atomicRequestId;
	}
	
	public  List[] getResults() {
		return results;
	}

	public  String[] getColumnNames() {
        return this.columnNames;
	}

	public String[] getDataTypes() {
        return this.dataTypes;
	}
		
    public Date getProcessingTimestamp() {
        return this.processingTimestamp;
    }

    public Date getCompletedTimestamp() {
        return this.completedTimestamp;
    }
    
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        atomicRequestId = (AtomicRequestID)in.readObject();
        processingTimestamp = (Date)in.readObject();
        completedTimestamp = (Date)in.readObject();
        columnNames = ExternalizeUtil.readStringArray(in);
        dataTypes = ExternalizeUtil.readStringArray(in);
        results = BatchSerializer.readBatch(in, dataTypes);
        firstRow = in.readInt();
        lastRow = in.readInt();
        partialResultsFlag = in.readBoolean();
        finalRow = in.readInt();
        supportsImplicitClose = in.readBoolean();
        requestClosed = in.readBoolean();        
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(atomicRequestId);
        out.writeObject(processingTimestamp);
        out.writeObject(completedTimestamp);
        ExternalizeUtil.writeArray(out, columnNames);
        ExternalizeUtil.writeArray(out, dataTypes);
        BatchSerializer.writeBatch(out, dataTypes, results);
        out.writeInt(firstRow);
        out.writeInt(lastRow);
        out.writeBoolean(partialResultsFlag);
        out.writeInt(finalRow);
        out.writeBoolean(supportsImplicitClose);
        out.writeBoolean(requestClosed);
	}

	public boolean isTransactional() {
		return isTransactional;
	}

	public void setTransactional(boolean isTransactional) {
		this.isTransactional = isTransactional;
	}    
}
