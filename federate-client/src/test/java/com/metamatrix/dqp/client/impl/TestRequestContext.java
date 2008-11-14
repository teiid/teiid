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

package com.metamatrix.dqp.client.impl;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.dqp.message.RequestID;


/** 
 * @since 4.3
 */
public class TestRequestContext extends TestCase {
    
    private static final String PORTABLE_STRING = "RequestContext:update=false;executionID=42;connectionID=connectionIDString"; //$NON-NLS-1$
    private static final String PORTABLE_STRING_NO_CONNID = "RequestContext:update=true;executionID=42"; //$NON-NLS-1$

    public void testConstructor_RequestID() {
        RequestContext context = new RequestContext(new RequestID(42L), false);
        assertEquals(new RequestID(42L), context.getRequestID());
        assertFalse(context.isUpdate());
        
        context = new RequestContext(new RequestID("connectionIDString", 42L), true); //$NON-NLS-1$
        assertEquals(new RequestID("connectionIDString", 42L), context.getRequestID()); //$NON-NLS-1$
        assertTrue(context.isUpdate());
    }
    
    public void testConstructor_PortableContext() throws Exception {
        RequestContext context = new RequestContext(new SerializablePortableContext(PORTABLE_STRING_NO_CONNID));
        assertEquals(new RequestID(42L), context.getRequestID());
        assertTrue(context.isUpdate());
        assertEquals(PORTABLE_STRING_NO_CONNID, context.getPortableString());
        
        // With connection ID
        context = new RequestContext(new SerializablePortableContext(PORTABLE_STRING));
        assertEquals(new RequestID("connectionIDString", 42L), context.getRequestID()); //$NON-NLS-1$
        assertFalse(context.isUpdate());
        assertEquals(PORTABLE_STRING, context.getPortableString());
        
        // Bad input
        try {
            context = new RequestContext(new SerializablePortableContext("someString")); //$NON-NLS-1$
            fail("Should fail for invalid contexts"); //$NON-NLS-1$
        } catch (MetaMatrixProcessingException e) {
            assertEquals("Invalid portable context string. Unable to reinstantiate a request context from the string :someString", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testGetPortableString() throws Exception {
        RequestContext context = new RequestContext(new RequestID(42L), true);
        assertEquals(PORTABLE_STRING_NO_CONNID, context.getPortableString());
        
        context = new RequestContext(new RequestID("connectionIDString", 42L), false); //$NON-NLS-1$
        assertEquals(PORTABLE_STRING, context.getPortableString());
        
        context = new RequestContext(new SerializablePortableContext(PORTABLE_STRING_NO_CONNID));
        assertEquals(PORTABLE_STRING_NO_CONNID, context.getPortableString());
        
        // With connection ID
        context = new RequestContext(new SerializablePortableContext(PORTABLE_STRING));
        assertEquals(PORTABLE_STRING, context.getPortableString());
    }
    
    public void testCreateRequestContextFromPortableContext() throws Exception {
        RequestContext context = new RequestContext(new RequestID(42L), false);
        RequestContext newContext = RequestContext.createRequestContextFromPortableContext(context);
        assertTrue(newContext == context);
        
        // Create from a different implementation of PortableContext
        newContext = RequestContext.createRequestContextFromPortableContext(new SerializablePortableContext(context.getPortableString()));
        assertEquals(context.getRequestID(), newContext.getRequestID());
        assertEquals(context.isUpdate(), newContext.isUpdate());
        assertEquals(context.getPortableString(), newContext.getPortableString());
        
        try {
            newContext = RequestContext.createRequestContextFromPortableContext(new SerializablePortableContext("someString")); //$NON-NLS-1$
            fail("Should fail for invalid contexts"); //$NON-NLS-1$
        } catch (MetaMatrixProcessingException e) {
            assertEquals("Invalid portable context string. Unable to reinstantiate a request context from the string :someString", e.getMessage()); //$NON-NLS-1$
        }
    }
}
