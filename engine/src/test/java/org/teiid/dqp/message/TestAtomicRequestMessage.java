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

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.client.RequestMessage;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.TestQueryImpl;
import org.teiid.dqp.internal.process.DQPWorkContext;

@Ignore(value="Serialization of language objects has been turned off")
public class TestAtomicRequestMessage {

    public static AtomicRequestMessage example() {
        RequestMessage rm = new RequestMessage();
        DQPWorkContext workContext = new DQPWorkContext();
        workContext.getSession().setSessionId(String.valueOf(2));
        AtomicRequestMessage message = new AtomicRequestMessage(rm, workContext, 1000);
        message.setCommand(TestQueryImpl.helpExample(true));
        message.setFetchSize(100);
        message.setPartialResults(true);
        message.setRequestID(new RequestID(5000L));
        
        //AtomicRequestMessage-specific stuff
        message.setConnectorName("connectorBindingID"); //$NON-NLS-1$
        return message;
    }

    @Test public void testSerialize() throws Exception {
    	AtomicRequestMessage example = example();
    	AtomicRequestMessage copy = UnitTestUtil.helpSerialize(example);

        assertEquals(TestQueryImpl.helpExample(true), copy.getCommand());
        assertEquals(100, copy.getFetchSize());

        assertEquals(example.getProcessingTimestamp(), copy.getProcessingTimestamp());
        assertEquals(new RequestID(5000L), copy.getRequestID());
        assertEquals("2", copy.getWorkContext().getSessionId()); //$NON-NLS-1$
        //AtomicRequestMessage-specific stuff
        assertEquals("connectorBindingID", copy.getConnectorName()); //$NON-NLS-1$
        assertEquals(1000, copy.getAtomicRequestID().getNodeID());
    }
}
