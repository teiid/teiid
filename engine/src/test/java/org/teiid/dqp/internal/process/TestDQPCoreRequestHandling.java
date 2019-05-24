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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.client.RequestMessage;
import org.teiid.client.SourceWarning;
import org.teiid.core.TeiidException;
import org.teiid.dqp.internal.datamgr.FakeTransactionService;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.RequestID;
import org.teiid.query.sql.lang.Command;


/**
 */
public class TestDQPCoreRequestHandling extends TestCase {

    private static final String SESSION_STRING = "2"; //$NON-NLS-1$

    public TestDQPCoreRequestHandling(String name) {
        super(name);
    }

    private void compareReqInfos(Collection<RequestID> reqs1, Collection<RequestMetadata> reqs2) {
        Set<RequestID> reqIDs2 = new HashSet<RequestID>();
        for (RequestMetadata requestInfo : reqs2) {
            reqIDs2.add(new RequestID(requestInfo.getSessionId(), requestInfo.getExecutionId()));
        }

        assertEquals("Collections of request infos are not the same: ", new HashSet<RequestID>(reqs1), reqIDs2); //$NON-NLS-1$
    }

    /**
     * Test for Collection getRequests(SessionToken) - no requests
     */
    public void testGetRequestsSessionToken1() {
        DQPCore rm = new DQPCore();
        Set<RequestID> reqs = Collections.emptySet();
        Collection<RequestMetadata> actualReqs = rm.getRequestsForSession(SESSION_STRING);
        compareReqInfos(reqs, actualReqs);
    }

    /**
     * Test for Collection getRequests(SessionToken) - 1 request
     */
    public void testGetRequestsSessionToken2() {
        DQPCore rm = new DQPCore();
        rm.setTransactionService(new FakeTransactionService());
        Set<RequestID> reqs = new HashSet<RequestID>();
        RequestID id = addRequest(rm, SESSION_STRING, 1);
        reqs.add(id);

        Collection<RequestMetadata> actualReqs = rm.getRequestsForSession(SESSION_STRING);
        compareReqInfos(reqs, actualReqs);
    }

    private RequestID addRequest(DQPCore rm, String sessionId, int executionId) {
        RequestMessage r0 = new RequestMessage("test command"); //$NON-NLS-1$
        RequestID id = new RequestID(sessionId, executionId);
        addRequest(rm, r0, id, null, null);
        return id;
    }

    /**
     * Test for Collection getRequests(SessionToken) - 3 requests
     */
    public void testGetRequestsSessionToken3() {
        DQPCore rm = new DQPCore();
        rm.setTransactionService(new FakeTransactionService());
        Set<RequestID> reqs = new HashSet<RequestID>();

        reqs.add(addRequest(rm, SESSION_STRING, 0));
        reqs.add(addRequest(rm, SESSION_STRING, 1));
        reqs.add(addRequest(rm, SESSION_STRING, 2));

        Collection<RequestMetadata> actualReqs = rm.getRequestsForSession(SESSION_STRING);
        compareReqInfos(reqs, actualReqs);
    }

    private SourceWarning getSourceFailures(String model, String binding, String message) {
        return new SourceWarning(model, binding, new TeiidException(message), true);
    }

    public void testAddRequest() {
        DQPCore rm = new DQPCore();
        rm.setTransactionService(new FakeTransactionService());
        RequestMessage r0 = new RequestMessage("foo"); //$NON-NLS-1$
        RequestID requestID = new RequestID(SESSION_STRING, 1);
        addRequest(rm, r0, requestID, null, null);
    }

    static RequestWorkItem addRequest(DQPCore rm,
                    RequestMessage requestMsg,
                    RequestID id,
                    Command originalCommand,
                    DQPWorkContext workContext) {

        if (workContext == null) {
            workContext = new DQPWorkContext();
            workContext.getSession().setSessionId(id.getConnectionID());
            workContext.getSession().setUserName("foo"); //$NON-NLS-1$
        }
        RequestWorkItem workItem = new RequestWorkItem(rm, requestMsg, null, null, id, workContext);
        workItem.setOriginalCommand(originalCommand);
        ClientState state = rm.getClientState(id.getConnectionID(), true);
        rm.addRequest(id, workItem, state);
        return workItem;
    }

    public void testGetConnectorInfo() {
        DQPCore rm = new DQPCore();
        rm.setTransactionService(new FakeTransactionService());
        RequestMessage r0 = new RequestMessage("foo"); //$NON-NLS-1$
        RequestID requestID = new RequestID(SESSION_STRING, 1);
        RequestWorkItem workItem = addRequest(rm, r0, requestID, null, null);
        AtomicRequestMessage atomicReq = new AtomicRequestMessage(workItem.requestMsg, workItem.getDqpWorkContext(), 1);

        DataTierTupleSource info = Mockito.mock(DataTierTupleSource.class);
        workItem.addConnectorRequest(atomicReq.getAtomicRequestID(), info);

        DataTierTupleSource arInfo = workItem.getConnectorRequest(atomicReq.getAtomicRequestID());
        assertTrue(arInfo == info);
    }

    public void testRemoveConnectorInfo() {
        DQPCore rm = new DQPCore();
        rm.setTransactionService(new FakeTransactionService());
        RequestMessage r0 = new RequestMessage("foo"); //$NON-NLS-1$
        RequestID requestID = new RequestID(SESSION_STRING, 1);
        RequestWorkItem workItem = addRequest(rm, r0, requestID, null, null);
        AtomicRequestMessage atomicReq = new AtomicRequestMessage(workItem.requestMsg, workItem.getDqpWorkContext(), 1);

        DataTierTupleSource info = Mockito.mock(DataTierTupleSource.class);
        workItem.addConnectorRequest(atomicReq.getAtomicRequestID(), info);

        workItem.closeAtomicRequest(atomicReq.getAtomicRequestID());

        DataTierTupleSource arInfo = workItem.getConnectorRequest(atomicReq.getAtomicRequestID());
        assertNull(arInfo);
    }
}
