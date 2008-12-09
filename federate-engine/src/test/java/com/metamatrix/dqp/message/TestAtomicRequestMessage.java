/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.message;

import java.util.Date;

import junit.framework.TestCase;

import com.metamatrix.core.util.TestExternalizeUtil;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;
import com.metamatrix.dqp.internal.datamgr.language.TestQueryImpl;
import com.metamatrix.dqp.internal.process.DQPWorkContext;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;

public class TestAtomicRequestMessage extends TestCase {

    /**
     * Constructor for TestAtomicRequestMessage.
     * @param name
     */
    public TestAtomicRequestMessage(String name) {
        super(name);
    }

    public static AtomicRequestMessage example() {
        RequestMessage rm = new RequestMessage();
        DQPWorkContext workContext = new DQPWorkContext();
        workContext.setSessionId(new MetaMatrixSessionID(2));
        AtomicRequestMessage message = new AtomicRequestMessage(rm, workContext, 1000);
        message.setCommand(TestQueryImpl.helpExample());
        message.setFetchSize(100);
        message.setPartialResults(true);
        message.setProcessingTimestamp(new Date(12345678L));
        message.setRequestID(new RequestID(5000L));
        
        //AtomicRequestMessage-specific stuff
        message.setConnectorBindingID("connectorBindingID"); //$NON-NLS-1$
        message.setConnectorID(new ConnectorID("10000")); //$NON-NLS-1$
        return message;
    }

    public void testSerialize() throws Exception {
        Object serialized = TestExternalizeUtil.helpSerializeRoundtrip(example());
        assertNotNull(serialized);
        assertTrue(serialized instanceof AtomicRequestMessage);
        AtomicRequestMessage copy = (AtomicRequestMessage)serialized;

        assertEquals(TestQueryImpl.helpExample(), copy.getCommand());
        assertEquals(100, copy.getFetchSize());

        assertEquals(new Date(12345678L), copy.getProcessingTimestamp());
        assertEquals(new RequestID(5000L), copy.getRequestID());
        assertEquals("00000000-0000-0002-0000-000000000002", copy.getWorkContext().getConnectionID()); //$NON-NLS-1$
        //AtomicRequestMessage-specific stuff
        assertEquals("connectorBindingID", copy.getConnectorBindingID()); //$NON-NLS-1$
        assertEquals(new ConnectorID("10000"), copy.getConnectorID()); //$NON-NLS-1$
        assertEquals(1000, copy.getAtomicRequestID().getNodeID());
    }
}
