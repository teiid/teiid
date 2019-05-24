/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
