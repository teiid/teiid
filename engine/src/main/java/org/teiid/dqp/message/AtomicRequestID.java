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

import org.teiid.core.util.HashCodeUtil;



/**
 * This class uniquely identifies a AtomicRequestMessage 
 */
public class AtomicRequestID implements Externalizable {
    private static final String SEPARATOR = "."; //$NON-NLS-1$

    private RequestID requestId;
    private int nodeId;
    private int executionId;

    public AtomicRequestID() {
        // This is only to honor the externalizable interface..
    }
           
    public AtomicRequestID(RequestID requestId, int nodeId, int execId) {
        this.requestId = requestId;
        this.nodeId = nodeId;
        this.executionId = execId;
    }
    
    public RequestID getRequestID() {
        return requestId;
    }
    
    public int getNodeID() {
        return nodeId;
    }

    public int getExecutionId() {
        return executionId;
    }

    /**
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {        
        this.requestId = (RequestID)in.readObject();
        this.nodeId = in.readInt();
        this.executionId = in.readInt();
    }

    /**
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.requestId);
        out.writeInt(this.nodeId);
        out.writeInt(this.executionId);
    }    
    
    public boolean equals(Object o) {
        if (this == o) return true;
        
        if (!(o instanceof AtomicRequestID)) return false;

        final AtomicRequestID c = (AtomicRequestID) o;
        
        return (c.requestId.equals(this.requestId) && c.nodeId == this.nodeId && c.executionId == this.executionId);
    }

    public int hashCode() {
        return HashCodeUtil.hashCode(HashCodeUtil.hashCode(this.requestId.hashCode(), this.nodeId), this.executionId);
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(this.requestId).append(SEPARATOR).append(this.nodeId).append(SEPARATOR).append(this.executionId); 
        return sb.toString(); 
    }
}
