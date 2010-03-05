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
 * Time: 5:36:02 PM
 */
package org.teiid.dqp.internal.datamgr.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.dqp.internal.datamgr.impl.TestConnectorWorkItem.QueueResultsReceiver;

import com.metamatrix.common.queue.FakeWorkManager;
import com.metamatrix.dqp.message.AtomicRequestMessage;

/**
 * JUnit test for TestConnectorManagerImpl
 */
public final class TestConnectorManagerImpl {
   
	private ConnectorEnvironment helpGetAppProps() {
		ConnectorEnvironment env = Mockito.mock(ConnectorEnvironment.class);
		Mockito.stub(env.getMaxResultRows()).toReturn(10);
		return env;
    }
	
	static ConnectorManager getConnectorManager(ConnectorEnvironment env) throws Exception {
		final FakeConnector c = new FakeConnector();
		c.setConnectorEnvironment(env);		
		ConnectorManager cm = new ConnectorManager("FakeConnector") {
			Connector getConnector() {
				return c;
			}
		};
		cm.start();
		return cm;
	}	

    // =========================================================================
    //                         T E S T   C A S E S
    // =========================================================================


    @Test public void testReceive() throws Exception {
    	ConnectorManager cm = getConnectorManager(helpGetAppProps());
        AtomicRequestMessage request = TestConnectorWorkItem.createNewAtomicRequestMessage(1, 1);
        QueueResultsReceiver receiver = new QueueResultsReceiver();
        cm.executeRequest(new FakeWorkManager(), receiver, request);
        assertNotNull(receiver.getResults().poll(1000, TimeUnit.MILLISECONDS));
        cm.stop();
    }
    
    
    @Test public void testDefect19049() throws Exception {
        ConnectorManager cm = getConnectorManager(helpGetAppProps());
        AtomicRequestMessage request = TestConnectorWorkItem.createNewAtomicRequestMessage(1, 1);
        QueueResultsReceiver receiver = new QueueResultsReceiver();
        cm.executeRequest(new FakeWorkManager(),receiver, request);
        assertNotNull(receiver.getResults().poll(1000, TimeUnit.MILLISECONDS));
        cm.stop();
    }
}