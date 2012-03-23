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

package org.teiid.adminapi;




/** 
 * When a user submits a SQL command to the system for processing, usually that represents
 * a single request. A single request might have one or more source
 * requests (the requests that are being processed on the physical data sources) as part
 * of original request. 
 * 
 *  <p>A request is identified by a numbers separated by '|'. usually in they are arranged 
 *  in the pattern [session]|[request] or [session]|[request]|[source request] </p>
 */
public interface Request extends AdminObject, DomainAware {
	
	public enum ProcessingState {
		PROCESSING,
		DONE,
		CANCELED
	}
	
	public enum ThreadState {
		RUNNING, 
		QUEUED, 
		IDLE
	}

    /**
     * Get the ExecutionId for a Request
     * @return ExecutionId
     */
    public long getExecutionId();
    
    /**
     * Get the SessionID for a Request
     * 
     * @return String SessionID
     */
    public String getSessionId();

    /**
     * Get the SQL Command sent to the Server for a Request
     * 
     * @return SQL Command
     */
    public String getCommand();

    /**
     * Get when the processing began for this Request
     * @return Date processing began
     */
    public long getStartTime();

    /**
     * Get the TransactionID of the Request
     * 
     * @return String of TransactionID if in a transaction
     */
    public String getTransactionId();
    
    /** 
     * @return Returns whether this is a Source Request.
     */
    public boolean sourceRequest();
    
    /**  
     * @return In the case that this is a source request this represents the node id. Otherwise null
     */
    public Integer getNodeId();

    /**  
     * @return The request state
     */
	ProcessingState getState();

    /**  
     * @return The thread state
     */
	ThreadState getThreadState();
    
}
