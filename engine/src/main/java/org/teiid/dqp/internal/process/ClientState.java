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

package org.teiid.dqp.internal.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.dqp.message.RequestID;
import org.teiid.query.tempdata.TempTableStore;

/**
 *
 */
//TODO: merge with DQPWorkContext
class ClientState {
    LinkedHashSet<RequestID> requests;
    TempTableStore sessionTables;
    volatile SessionMetadata session;

    public ClientState(TempTableStore tableStoreImpl) {
        this.sessionTables = tableStoreImpl;
    }

    public synchronized void addRequest(RequestID requestID) {
        if (requests == null) {
            requests = new LinkedHashSet<RequestID>(2);
        }
        requests.add(requestID);
    }

    public synchronized List<RequestID> getRequests() {
        if (requests == null) {
            return Collections.emptyList();
        }
        return new ArrayList<RequestID>(requests);
    }

    public synchronized void removeRequest(RequestID requestID) {
        if (requests != null) {
            requests.remove(requestID);
        }
    }
}