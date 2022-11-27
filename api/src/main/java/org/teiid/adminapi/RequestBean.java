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

package org.teiid.adminapi;

import org.teiid.adminapi.Request.ProcessingState;
import org.teiid.adminapi.Request.ThreadState;

public interface RequestBean {

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
    public boolean isSourceRequest();

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
