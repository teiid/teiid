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
package org.teiid.dqp.internal.datamgr;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.teiid.dqp.message.AtomicRequestID;
import org.teiid.dqp.message.AtomicRequestMessage;
import org.teiid.dqp.message.RequestID;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;


/**
 * JUnit test for TestConnectorStateManager
 */
public final class TestConnectorManager {
    private AtomicRequestMessage request;
    private ConnectorManager csm;
    
	static ConnectorManager getConnectorManager() throws Exception {
		final FakeConnector c = new FakeConnector();
		ConnectorManager cm = new ConnectorManager("FakeConnector","FakeConnector") { //$NON-NLS-1$ //$NON-NLS-2$
			public ExecutionFactory getExecutionFactory() {
				return c;
			}
			public Object getConnectionFactory(){
				return c;
			}
		};
		cm.start();
		return cm;
	}

	@Before
    public void setUp() throws Exception {
        request = TestConnectorWorkItem.createNewAtomicRequestMessage(1, 1);
        csm = getConnectorManager();
    }

    void helpAssureOneState() throws Exception {
    	csm.registerRequest(request);
    	ConnectorWork state = csm.getState(request.getAtomicRequestID());
    	assertEquals(state, csm.getState(request.getAtomicRequestID()));
    }

    @Test public void testCreateAndAddRequestState() throws Exception {
        helpAssureOneState();
        assertEquals("Expected size of 1", 1, csm.size()); //$NON-NLS-1$
    }

    @Test public void testIllegalCreate() throws Exception {
        helpAssureOneState();
        try {
        	helpAssureOneState();
        	fail("expected exception"); //$NON-NLS-1$
        } catch (AssertionError e) {
        	assertEquals("State already existed", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testRemoveRequestState() throws Exception {
        helpAssureOneState();
        csm.removeState(request.getAtomicRequestID());
        assertEquals("Expected size of 0", 0, csm.size()); //$NON-NLS-1$
    }

    @Test public void testRemoveUnknownRequestState() throws Exception {
        helpAssureOneState();
        csm.removeState(new AtomicRequestID(new RequestID("ZZZZ", 3210), 5, 5)); //$NON-NLS-1$

        assertEquals("Expected size of 1", 1, csm.size()); //$NON-NLS-1$
    }
    
    @Test public void testGetCapabilities() throws Exception {
    	final Object cf = new Object();
    	ExecutionFactory ef = new ExecutionFactory() {
    		public boolean isSourceRequiredForCapabilities() {
    			return true;
    		}
    	};
    	final Object[] cfHolder = new Object[1]; 
		ConnectorManager cm = new ConnectorManager("FakeConnector","FakeConnector", ef) { //$NON-NLS-1$ //$NON-NLS-2$
			
			public Object getConnectionFactory(){
				return cfHolder[0];
			}
		};
		cm.start();
		try {
			cm.getCapabilities();
			fail();
		} catch (TranslatorException e) {
			
		}
		ef = new ExecutionFactory() {
			
    		public boolean isSourceRequiredForCapabilities() {
    			return true;
    		}
    		@Override
    		public Object getConnection(Object factory,
    				ExecutionContext executionContext)
    				throws TranslatorException {
    			assertEquals(cf, factory);
    			return factory;
    		}
    		@Override
    		public void closeConnection(Object connection, Object factory) {
        	}
		};
		cm = new ConnectorManager("FakeConnector","FakeConnector", ef) { //$NON-NLS-1$ //$NON-NLS-2$
			
			public Object getConnectionFactory(){
				return cfHolder[0];
			}
		};
		cfHolder[0] = cf;
		cm.getCapabilities();
    }
           
}