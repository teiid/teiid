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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.core.util.TestExternalizeUtil;
import com.metamatrix.dqp.internal.datamgr.language.TestQueryImpl;

public class TestRequestMessage extends TestCase {

    /**
     * Constructor for TestRequestMessage.
     * @param name
     */
    public TestRequestMessage(String name) {
        super(name);
    }

    public static RequestMessage example() {
        RequestMessage message = new RequestMessage();
        message.setCallableStatement(true);
        message.setCommand(TestQueryImpl.helpExample());
        message.setFetchSize(100);
        List params = new ArrayList();
        params.add(new Integer(100));
        params.add(new Integer(200));
        params.add(new Integer(300));
        params.add(new Integer(400));
        message.setParameterValues(params);

        message.setPartialResults(true);
        message.setPreparedStatement(false);
        message.setSubmittedTimestamp(new Date(11111111L));
        message.setProcessingTimestamp(new Date(12345678L));
        message.setStyleSheet("myStyleSheet"); //$NON-NLS-1$
        message.setExecutionPayload("myExecutionPayload"); //$NON-NLS-1$
        message.setTxnAutoWrapMode("txnAutoWrapMode"); //$NON-NLS-1$

        message.setValidationMode(true);
        message.setXMLFormat("xMLFormat"); //$NON-NLS-1$
        message.setShowPlan(true);
        message.setRowLimit(1313);
        return message;
    }

    public void testSerialize() throws Exception {
        Object serialized = TestExternalizeUtil.helpSerializeRoundtrip(example());
        assertNotNull(serialized);
        assertTrue(serialized instanceof RequestMessage);
        RequestMessage copy = (RequestMessage)serialized;

        assertTrue(copy.isCallableStatement());
        assertEquals(TestQueryImpl.helpExample(), copy.getCommand());
        assertEquals(100, copy.getFetchSize());
        assertNotNull(copy.getParameterValues());
        assertEquals(4, copy.getParameterValues().size());
        assertEquals(new Integer(100), copy.getParameterValues().get(0));
        assertEquals(new Integer(200), copy.getParameterValues().get(1));
        assertEquals(new Integer(300), copy.getParameterValues().get(2));
        assertEquals(new Integer(400), copy.getParameterValues().get(3));

        assertFalse(copy.isPreparedStatement());
        assertEquals(new Date(11111111L), copy.getSubmittedTimestamp());
        assertEquals(new Date(12345678L), copy.getProcessingTimestamp());
        assertEquals("myStyleSheet", copy.getStyleSheet()); //$NON-NLS-1$
        assertEquals("myExecutionPayload", copy.getExecutionPayload()); //$NON-NLS-1$
        assertEquals("txnAutoWrapMode", copy.getTxnAutoWrapMode()); //$NON-NLS-1$
        assertTrue(copy.getValidationMode());
        assertEquals("xMLFormat", copy.getXMLFormat()); //$NON-NLS-1$
        assertTrue(copy.getShowPlan());
        assertEquals(1313, copy.getRowLimit());
        
    }

}
