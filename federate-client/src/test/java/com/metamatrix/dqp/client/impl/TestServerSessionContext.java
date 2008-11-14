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

import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.core.util.UnitTestUtil;

/** 
 * @since 4.3
 */
public class TestServerSessionContext extends TestCase {

    private static final String INFO_STRING = "ServerConnectionInfo:serverURL=mm://myhost:31000;user=metamatrixadmin;password=password;VirtualDatabaseName=VDB;VirtualDatabaseVersion=3"; //$NON-NLS-1$
    private static final String CONNECTION_CONTEXT = "myConnectionContextString"; //$NON-NLS-1$
    private static final String SESSION_PORTABLE_STRING = "ServerSessionContext:"+INFO_STRING+"|"+CONNECTION_CONTEXT; //$NON-NLS-1$ //$NON-NLS-2$
    
    public void testConstructor_ServerConnectionInfo() throws Exception {
        ServerConnectionInfo info = new ServerConnectionInfo(INFO_STRING);
        ServerSessionContext context = new ServerSessionContext(info, CONNECTION_CONTEXT);
        assertEquals(CONNECTION_CONTEXT, context.getConnectionContext());
        assertEquals("metamatrixadmin", context.getUserName()); //$NON-NLS-1$
        
        Properties props = new Properties();
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.SERVER_URL, "mm://myhost:31000"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.USER_PROP, "metamatrixadmin"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.PWD_PROP, "password"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.VDB_NAME, "VDB"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.VDB_VERSION, "3"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.HOST, "myhost"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.PORT, "31000"); //$NON-NLS-1$
        props.setProperty("vdbVersion", "3"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("vdbName", "VDB"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(props, context.getConnectionProperties());
        assertNull(context.getInstanceContext());
        
        assertEquals(SESSION_PORTABLE_STRING, context.getPortableString());
    }
    
    public void testConstructor_PortableContext() throws Exception {
        ServerSessionContext context = new ServerSessionContext(new SerializablePortableContext(SESSION_PORTABLE_STRING));
        assertEquals(CONNECTION_CONTEXT, context.getConnectionContext());
        assertEquals("metamatrixadmin", context.getUserName()); //$NON-NLS-1$
        
        Properties props = new Properties();
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.SERVER_URL, "mm://myhost:31000"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.USER_PROP, "metamatrixadmin"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.PWD_PROP, "password"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.VDB_NAME, "VDB"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.VDB_VERSION, "3"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.HOST, "myhost"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.PORT, "31000"); //$NON-NLS-1$
        props.setProperty("vdbVersion", "3"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("vdbName", "VDB"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(props, context.getConnectionProperties());
        assertNull(context.getInstanceContext());
        
        assertEquals(SESSION_PORTABLE_STRING, context.getPortableString());
    }
    
    public void testGetPortableString() throws Exception {
        ServerConnectionInfo info = new ServerConnectionInfo(INFO_STRING);
        ServerSessionContext context = new ServerSessionContext(info, CONNECTION_CONTEXT);
        assertEquals(SESSION_PORTABLE_STRING, context.getPortableString());
    }
    
    public void testEquals() throws Exception {
        ServerSessionContext context = new ServerSessionContext(new SerializablePortableContext(SESSION_PORTABLE_STRING));
        ServerConnectionInfo info = new ServerConnectionInfo(INFO_STRING);
        ServerSessionContext context2 = new ServerSessionContext(info, CONNECTION_CONTEXT);
        
        UnitTestUtil.helpTestEquivalence(0, context, context2);
        context2.setInstanceContext("myInstanceContext"); //$NON-NLS-1$
        // Instance context shouldn't matter
        UnitTestUtil.helpTestEquivalence(0, context, context2);
    }
    
    public void testCreateSessionContextFromPortableContext() throws Exception {
        ServerSessionContext context = new ServerSessionContext(new SerializablePortableContext(SESSION_PORTABLE_STRING));
        ServerSessionContext newContext = ServerSessionContext.createSessionContextFromPortableContext(context);
        assertTrue(newContext == context);
        
        newContext = ServerSessionContext.createSessionContextFromPortableContext(new SerializablePortableContext(SESSION_PORTABLE_STRING));
        assertEquals(context, newContext);
        
        try {
            newContext = ServerSessionContext.createSessionContextFromPortableContext(new SerializablePortableContext("hello")); //$NON-NLS-1$
            fail("Should fail for invalid contexts"); //$NON-NLS-1$
        } catch (MetaMatrixProcessingException e) {
            assertEquals("Invalid portable context string. Unable to reinstantiate a server session context from the string :hello", e.getMessage()); //$NON-NLS-1$
        }
    }

}
