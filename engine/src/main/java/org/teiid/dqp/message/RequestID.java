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

package org.teiid.dqp.message;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;

/**
 * <p>This class represents an identifier for a request.  However, there are some
 * differences in what constitutes "uniqueness" for a given RequestID that 
 * is based on context (where the id is used).  The RequestID has 2 parts:
 * connectionID, and executionIDFor the purposes of the RequestID, the combined 
 * representation is "connectionID.executionID" - this implies a scoping
 * for the name parts.  The connectionID specifies a particular connection that 
 * is making requests.  Each connection generates a unique executionID for each 
 * request execution, so the executionID is only unique in the context of a 
 * connectionID.  </p>
 * 
 * <p>When this class is used between client and server, the connectionID is implied
 * and thus only the executionID part will be used.  The server will qualify the 
 * executionID with a connectionID when it reaches the server.  </p>
 * 
 * <p>RequestIDs are immutable so no setters exist.  This allows hashcodes to be 
 * pre-computed for faster comparison in equals.</p>  
 */
public class RequestID implements Externalizable {

    static final long serialVersionUID = -2888539138291776071L;
    
    public static final String NO_CONNECTION_STR = "C"; //$NON-NLS-1$
    private static final String SEPARATOR = "."; //$NON-NLS-1$

    // Basic state
    private String connectionID;
    private long executionID;
    
    // Derived state
    private String combinedID;

    /**
     * Necessary for implementing Externalizable 
     */
    public RequestID() {        
    }
    
    /**
     * Create a RequestID using all of the ID parts.
     * @param connectionID Identifies a connection, may be null
     * @param executionID Identifies an execution, cannot be null
     */
    public RequestID(String connectionID, long executionID) {
        this.connectionID = connectionID;
        this.executionID = executionID;
    }
    
    public RequestID(long connectionID, long executionID) {
        this.connectionID = String.valueOf(connectionID);
        this.executionID = executionID;
    }    
    
    /**
     * Create a RequestID for an execution where the connection is 
     * not specified.
     * @param executionID Identifies an execution, cannot be null
     */
    public RequestID(long executionID) {
        this(null, executionID);
    }

    
    /**
     * Return connectionID, may be null if connection has not been specified.
     * @return Connection ID, may be null
     */
    public String getConnectionID() {
        return this.connectionID;
    }
    
    /**
     * Return executionID, which identifies a per-connection execution.
     * @return Execution ID
     */
    public long getExecutionID() {
        return this.executionID;
    }

    /**
     * Create a unique combined ID string from the RequestID parts. 
     */
    private void createCombinedID() {
        StringBuffer combinedStr = new StringBuffer();
        if(this.connectionID != null) {
            combinedStr.append(this.connectionID);
        } else {
            combinedStr.append(NO_CONNECTION_STR);
        }
        combinedStr.append(SEPARATOR);               
        combinedStr.append(this.executionID);
                
        this.combinedID = combinedStr.toString();
    }
    
    public int hashCode() {
        return HashCodeUtil.hashCode(connectionID==null?0:connectionID.hashCode(), executionID);
    }
    
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        } else if(obj == null || !(obj instanceof RequestID)) {
            return false;
        } 
        RequestID other = (RequestID)obj;
        return this.executionID == other.executionID 
        	&& EquivalenceUtil.areEqual(this.connectionID, other.connectionID);
    }
    
    /**
     * Return a combined string for the ID.
     */
    public String toString() {
    	if (combinedID == null) {
    		createCombinedID();
    	}
        return this.combinedID;
    }

    /**
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        connectionID = (String)in.readObject();
        executionID = in.readLong();
    }

    /**
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(connectionID);
        out.writeLong(executionID);
    }
}
