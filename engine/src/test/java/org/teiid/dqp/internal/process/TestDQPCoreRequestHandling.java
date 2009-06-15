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

package org.teiid.dqp.internal.process;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.mockito.Mockito;
import org.teiid.connector.xa.api.TransactionContext;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.DataTierTupleSource;
import org.teiid.dqp.internal.process.RequestWorkItem;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.dqp.exception.SourceWarning;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.message.RequestMessage;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.server.serverapi.RequestInfo;

/**
 */
public class TestDQPCoreRequestHandling extends TestCase {

	private static final String SESSION_STRING = new MetaMatrixSessionID(2).toString();
	
    public TestDQPCoreRequestHandling(String name) {
        super(name);
    }

    private void compareReqInfos(Collection<RequestID> reqs1, Collection<RequestInfo> reqs2) {
        Set reqIDs2 = new HashSet();
        for (RequestInfo requestInfo : reqs2) {
            reqIDs2.add(requestInfo.getRequestID());
        }
        
        assertEquals("Collections of request infos are not the same: ", new HashSet(reqs1), reqIDs2); //$NON-NLS-1$
    }

    /**
     * Test for Collection getRequests(SessionToken) - no requests
     */
    public void testGetRequestsSessionToken1() {
        DQPCore rm = new DQPCore();
        Set reqs = new HashSet();                
        Collection actualReqs = rm.getRequestsByClient("foo");
        compareReqInfos(reqs, actualReqs);
    }

    /**
     * Test for Collection getRequests(SessionToken) - 1 request
     */
    public void testGetRequestsSessionToken2() {
    	DQPCore rm = new DQPCore();
    	Set reqs = new HashSet();
        RequestID id = addRequest(rm, SESSION_STRING, 1);
        reqs.add(id);

        Collection<RequestInfo> actualReqs = rm.getRequestsByClient(SESSION_STRING);
        compareReqInfos(reqs, actualReqs);
    }

	private RequestID addRequest(DQPCore rm, String sessionId, int executionId) {
		RequestMessage r0 = new RequestMessage("test command"); //$NON-NLS-1$
        RequestID id = new RequestID(sessionId, executionId);
        addRequest(rm, r0, id, null, null);  //$NON-NLS-1$
		return id;
	}

    /**
     * Test for Collection getRequests(SessionToken) - 3 requests
     */
    public void testGetRequestsSessionToken3() {
        DQPCore rm = new DQPCore();
        Set reqs = new HashSet();
         
        reqs.add(addRequest(rm, SESSION_STRING, 0));
        reqs.add(addRequest(rm, SESSION_STRING, 1));
        reqs.add(addRequest(rm, SESSION_STRING, 2));
                
        Collection actualReqs = rm.getRequestsByClient(SESSION_STRING);
        compareReqInfos(reqs, actualReqs);
    }
    
    private SourceWarning getSourceFailures(String model, String binding, String message) {
    	return new SourceWarning(model, binding, new MetaMatrixException(message), true);
    }
        
    public void testAddRequest() {
        DQPCore rm = new DQPCore();
        RequestMessage r0 = new RequestMessage("foo"); //$NON-NLS-1$
        RequestID requestID = new RequestID(SESSION_STRING, 1);
        RequestWorkItem workItem = addRequest(rm, r0, requestID, null, null);  //$NON-NLS-1$
        assertTrue(workItem.resultsCursor.resultsRequested);
    }
    
    public void testWarnings1() {
        DQPCore rm = new DQPCore();
        RequestMessage r0 = new RequestMessage("foo"); //$NON-NLS-1$
        RequestID requestID = new RequestID(SESSION_STRING, 1);

        RequestWorkItem workItem = addRequest(rm, r0, requestID, null, null);  //$NON-NLS-1$
                
        workItem.addSourceFailureDetails(getSourceFailures("Model1", "Binding1", "Warning1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        workItem.addSourceFailureDetails(getSourceFailures("Model2", "Binding2", "Warning2")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        workItem.addSourceFailureDetails(getSourceFailures("Model3", "Binding3", "Warning3")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        
        assertEquals(3, workItem.getWarnings().size());
    }
    
    static RequestWorkItem addRequest(DQPCore rm, 
                    RequestMessage requestMsg,
                    RequestID id,
                    Command originalCommand,
                    DQPWorkContext workContext) {
     
    	if (workContext == null) {
	    	workContext = new DQPWorkContext();
	    	workContext.setSessionToken(new SessionToken(new MetaMatrixSessionID(Long.valueOf(id.getConnectionID())), "foo")); //$NON-NLS-1$
    	}
        RequestWorkItem workItem = new RequestWorkItem(rm, requestMsg, null, null, id, workContext);
        workItem.setOriginalCommand(originalCommand);
        rm.addRequest(id, workItem);
        return workItem;
    }
    
    public void testGetConnectorInfo() {
        DQPCore rm = new DQPCore();
        RequestMessage r0 = new RequestMessage("foo"); //$NON-NLS-1$
        RequestID requestID = new RequestID(SESSION_STRING, 1);
        RequestWorkItem workItem = addRequest(rm, r0, requestID, null, null);
        AtomicRequestMessage atomicReq = new AtomicRequestMessage(workItem.requestMsg, workItem.dqpWorkContext, 1);

        DataTierTupleSource info = new DataTierTupleSource(null, atomicReq, null, new ConnectorID("connID"), workItem); //$NON-NLS-1$
        workItem.addConnectorRequest(atomicReq.getAtomicRequestID(), info);
        
        DataTierTupleSource arInfo = workItem.getConnectorRequest(atomicReq.getAtomicRequestID());
        assertTrue(arInfo == info);
    }
    
    public void testRemoveConnectorInfo() {
        DQPCore rm = new DQPCore();
        RequestMessage r0 = new RequestMessage("foo"); //$NON-NLS-1$
        RequestID requestID = new RequestID(SESSION_STRING, 1);
        RequestWorkItem workItem = addRequest(rm, r0, requestID, null, null);
        AtomicRequestMessage atomicReq = new AtomicRequestMessage(workItem.requestMsg, workItem.dqpWorkContext, 1);

        DataTierTupleSource info = new DataTierTupleSource(null, atomicReq, null, new ConnectorID("connID"), workItem); //$NON-NLS-1$
        workItem.addConnectorRequest(atomicReq.getAtomicRequestID(), info);
        
        workItem.closeAtomicRequest(atomicReq.getAtomicRequestID());
        
        DataTierTupleSource arInfo = workItem.getConnectorRequest(atomicReq.getAtomicRequestID());
        assertNull(arInfo);
    }
}
