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

/*
 * Date: Sep 17, 2003
 * Time: 3:48:54 PM
 */
package org.teiid.dqp.internal.datamgr.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorWorkItem;
import org.teiid.dqp.internal.datamgr.impl.ConnectorWrapper;

import junit.framework.TestCase;

import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.dqp.message.AtomicRequestID;
import com.metamatrix.dqp.message.AtomicRequestMessage;
import com.metamatrix.dqp.message.AtomicResultsMessage;
import com.metamatrix.dqp.message.RequestID;

/**
 * JUnit test for TestConnectorStateManager
 */
public final class TestConnectorStateManager extends TestCase {
    private AtomicRequestMessage request;
    private ConnectorManager csm;

    /**
     * Constructor for TestConnectorStateManager.
     * @param name
     */
    public TestConnectorStateManager(final String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        request = TestConnectorWorkItem.createNewAtomicRequestMessage(1, 1);
        csm = new ConnectorManager();
        csm.setConnectorWorkerPool(Mockito.mock(WorkerPool.class));
        csm.setConnector(new ConnectorWrapper(new FakeConnector()));
        csm.setWorkItemFactory(new ConnectorWorkItemFactory(csm, null, true));
    }

    void helpAssureOneState() {
    	csm.executeRequest(null, request);
    	ConnectorWorkItem state = csm.getState(request.getAtomicRequestID());
    	assertEquals(state, csm.getState(request.getAtomicRequestID()));
    }

    public void testCreateAndAddRequestState() {
        helpAssureOneState();
        assertEquals("Expected size of 1", 1, csm.size()); //$NON-NLS-1$
    }

    public void testIllegalCreate() throws Exception {
        helpAssureOneState();
        try {
        	helpAssureOneState();
        	fail("expected exception"); //$NON-NLS-1$
        } catch (AssertionError e) {
        	assertEquals("State already existed", e.getMessage()); //$NON-NLS-1$
        }
    }

    public void testRemoveRequestState() {
        helpAssureOneState();
        csm.removeState(request.getAtomicRequestID());
        assertEquals("Expected size of 0", 0, csm.size()); //$NON-NLS-1$
    }

    public void testRemoveUnknownRequestState() {
        helpAssureOneState();
        csm.removeState(new AtomicRequestID(new RequestID("ZZZZ", 3210), 5, 5)); //$NON-NLS-1$

        assertEquals("Expected size of 1", 1, csm.size()); //$NON-NLS-1$
    }
    
    public void testStop() throws Exception {
    	List<ResultsFuture<AtomicResultsMessage>> futures = new ArrayList<ResultsFuture<AtomicResultsMessage>>();
    	for (int i=0; i<20; i++) {
    		ResultsFuture<AtomicResultsMessage> future = new ResultsFuture<AtomicResultsMessage>();
        	csm.executeRequest(future.getResultsReceiver(), TestConnectorWorkItem.createNewAtomicRequestMessage(i, 1));
        }

        csm.stop();
        
        for (ResultsFuture<AtomicResultsMessage> resultsFuture : futures) {
			assertTrue(resultsFuture.isDone());
			try {
				resultsFuture.get(1000, TimeUnit.MILLISECONDS);
			} catch (ExecutionException e) {
				
			}
		}
    }
    
}