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

import java.util.HashMap;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.core.util.UnitTestUtil;


/** 
 * @since 4.3
 */
public class TestServerConnectionInfo extends TestCase {
    
    private static final String BASE_PORTABLE_STRING = "ServerConnectionInfo:serverURL=mm://myhost:31000;user=metamatrixadmin;password=password;VirtualDatabaseName=VDB;VirtualDatabaseVersion=3"; //$NON-NLS-1$
    private static final String BASE_PORTABLE_STRING_NO_VERSION = "ServerConnectionInfo:serverURL=mm://myhost:31000;user=metamatrixadmin;password=password;VirtualDatabaseName=VDB"; //$NON-NLS-1$
    private static final String ENCODED_TRUSTED_PAYLOAD = ";trustedPayload=rO0ABXQADnRydXN0ZWRQYXlsb2Fk"; //$NON-NLS-1$
    private static final String ENCODED_OPTIONAL_PROPERTIES = ";optionalProperties=rO0ABXNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAABdAARb3B0aW9uYWxQcm9wZXJ0eTFzcgARamF2YS5sYW5nLkludGVnZXIS4qCk94GHOAIAAUkABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwAAAAKng\\="; //$NON-NLS-1$

    
    public void testConstructor_PortableContext() throws Exception {
        ServerConnectionInfo info = new ServerConnectionInfo(BASE_PORTABLE_STRING);
        assertEquals("mm://myhost:31000", info.getServerUrl()); //$NON-NLS-1$
        assertEquals("metamatrixadmin", info.getUser()); //$NON-NLS-1$
        assertEquals("password", info.getPassword()); //$NON-NLS-1$
        assertEquals("VDB", info.getVDBName()); //$NON-NLS-1$
        assertEquals("3", info.getVDBVersion()); //$NON-NLS-1$
        assertNull(info.getTrustedPayload());
        assertNull(info.getOptionalProperties());
        
        info = new ServerConnectionInfo(BASE_PORTABLE_STRING_NO_VERSION);
        assertEquals("mm://myhost:31000", info.getServerUrl()); //$NON-NLS-1$
        assertEquals("metamatrixadmin", info.getUser()); //$NON-NLS-1$
        assertEquals("password", info.getPassword()); //$NON-NLS-1$
        assertEquals("VDB", info.getVDBName()); //$NON-NLS-1$
        assertNull(info.getVDBVersion());
        assertNull(info.getTrustedPayload());
        assertNull(info.getOptionalProperties());
        
        info = new ServerConnectionInfo(BASE_PORTABLE_STRING+ENCODED_TRUSTED_PAYLOAD);
        assertEquals("mm://myhost:31000", info.getServerUrl()); //$NON-NLS-1$
        assertEquals("metamatrixadmin", info.getUser()); //$NON-NLS-1$
        assertEquals("password", info.getPassword()); //$NON-NLS-1$
        assertEquals("VDB", info.getVDBName()); //$NON-NLS-1$
        assertEquals("3", info.getVDBVersion()); //$NON-NLS-1$
        assertEquals("trustedPayload", info.getTrustedPayload()); //$NON-NLS-1$
        assertNull(info.getOptionalProperties());
        
        HashMap props = new HashMap();
        props.put("optionalProperty1", new Integer(42)); //$NON-NLS-1$
        
        info = new ServerConnectionInfo(BASE_PORTABLE_STRING+ENCODED_TRUSTED_PAYLOAD+ENCODED_OPTIONAL_PROPERTIES);
        assertEquals("mm://myhost:31000", info.getServerUrl()); //$NON-NLS-1$
        assertEquals("metamatrixadmin", info.getUser()); //$NON-NLS-1$
        assertEquals("password", info.getPassword()); //$NON-NLS-1$
        assertEquals("VDB", info.getVDBName()); //$NON-NLS-1$
        assertEquals("3", info.getVDBVersion()); //$NON-NLS-1$
        assertEquals("trustedPayload", info.getTrustedPayload()); //$NON-NLS-1$
        assertEquals(props, info.getOptionalProperties());
        
        info = new ServerConnectionInfo(BASE_PORTABLE_STRING+ENCODED_OPTIONAL_PROPERTIES);
        assertEquals("mm://myhost:31000", info.getServerUrl()); //$NON-NLS-1$
        assertEquals("metamatrixadmin", info.getUser()); //$NON-NLS-1$
        assertEquals("password", info.getPassword()); //$NON-NLS-1$
        assertEquals("VDB", info.getVDBName()); //$NON-NLS-1$
        assertEquals("3", info.getVDBVersion()); //$NON-NLS-1$
        assertNull(info.getTrustedPayload());
        assertEquals(props, info.getOptionalProperties());
        
        try {
            info = new ServerConnectionInfo("IncompatibleString"); //$NON-NLS-1$
            fail("Should have failed due to incompatible portable string"); //$NON-NLS-1$
        } catch (MetaMatrixProcessingException e) {
            assertEquals("Invalid portable context string. Unable to reinstantiate a ServerConnectionInfo from the string :IncompatibleString", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    public void testGetConnectionProperties() throws Exception {
        ServerConnectionInfo info = new ServerConnectionInfo(BASE_PORTABLE_STRING);
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
        assertEquals(props, info.getConnectionProperties());
        
        info = new ServerConnectionInfo(BASE_PORTABLE_STRING_NO_VERSION);
        props = new Properties();
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.SERVER_URL, "mm://myhost:31000"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.USER_PROP, "metamatrixadmin"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.PWD_PROP, "password"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.VDB_NAME, "VDB"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.HOST, "myhost"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.PORT, "31000"); //$NON-NLS-1$
        props.setProperty("vdbName", "VDB"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(props, info.getConnectionProperties());
        
        
        info = new ServerConnectionInfo(BASE_PORTABLE_STRING+ENCODED_TRUSTED_PAYLOAD);
        props = new Properties();
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.SERVER_URL, "mm://myhost:31000"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.USER_PROP, "metamatrixadmin"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.PWD_PROP, "password"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.VDB_NAME, "VDB"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.VDB_VERSION, "3"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.HOST, "myhost"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.PORT, "31000"); //$NON-NLS-1$
        props.put(com.metamatrix.jdbc.api.ConnectionProperties.TRUSTED_PAYLOAD_PROP, "trustedPayload"); //$NON-NLS-1$
        props.setProperty("vdbVersion", "3"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("vdbName", "VDB"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(props, info.getConnectionProperties());
        
        info = new ServerConnectionInfo(BASE_PORTABLE_STRING+ENCODED_TRUSTED_PAYLOAD+ENCODED_OPTIONAL_PROPERTIES);
        props = new Properties();
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.SERVER_URL, "mm://myhost:31000"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.USER_PROP, "metamatrixadmin"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.PWD_PROP, "password"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.VDB_NAME, "VDB"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.VDB_VERSION, "3"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.HOST, "myhost"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.PORT, "31000"); //$NON-NLS-1$
        props.put(com.metamatrix.jdbc.api.ConnectionProperties.TRUSTED_PAYLOAD_PROP, "trustedPayload"); //$NON-NLS-1$
        props.put("optionalProperty1", new Integer(42)); //$NON-NLS-1$
        props.setProperty("vdbVersion", "3"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("vdbName", "VDB"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(props, info.getConnectionProperties());
        
        info = new ServerConnectionInfo(BASE_PORTABLE_STRING+ENCODED_OPTIONAL_PROPERTIES);
        props = new Properties();
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.SERVER_URL, "mm://myhost:31000"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.USER_PROP, "metamatrixadmin"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.PWD_PROP, "password"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.VDB_NAME, "VDB"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.VDB_VERSION, "3"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.HOST, "myhost"); //$NON-NLS-1$
        props.setProperty(com.metamatrix.jdbc.api.ConnectionProperties.PORT, "31000"); //$NON-NLS-1$
        props.put("optionalProperty1", new Integer(42)); //$NON-NLS-1$
        props.setProperty("vdbVersion", "3"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("vdbName", "VDB"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(props, info.getConnectionProperties());
    }
    
    public void testEquals() throws Exception {
        ServerConnectionInfo info = new ServerConnectionInfo();
        info.setServerUrl("mm://myhost:31000"); //$NON-NLS-1$
        info.setUser("metamatrixadmin"); //$NON-NLS-1$
        info.setPassword("password"); //$NON-NLS-1$
        info.setVDBName("VDB"); //$NON-NLS-1$
        info.setVDBVersion("3"); //$NON-NLS-1$
        
        UnitTestUtil.helpTestEquivalence(0, new ServerConnectionInfo(BASE_PORTABLE_STRING), info);
        info.setUser("newUser"); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(1, new ServerConnectionInfo(BASE_PORTABLE_STRING), info);
        
        info.setUser("metamatrixadmin"); //$NON-NLS-1$
        info.setVDBVersion(null);
        UnitTestUtil.helpTestEquivalence(0, new ServerConnectionInfo(BASE_PORTABLE_STRING_NO_VERSION), info);
        info.setVDBVersion("3"); //$NON-NLS-1$
        info.setTrustedPayload("trustedPayload"); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(0, new ServerConnectionInfo(BASE_PORTABLE_STRING + ENCODED_TRUSTED_PAYLOAD), info);
        UnitTestUtil.helpTestEquivalence(1, new ServerConnectionInfo(BASE_PORTABLE_STRING), info);
        
        info.setOptionalProperty("optionalProperty1", new Integer(42)); //$NON-NLS-1$
        UnitTestUtil.helpTestEquivalence(0, new ServerConnectionInfo(BASE_PORTABLE_STRING+ENCODED_TRUSTED_PAYLOAD+ENCODED_OPTIONAL_PROPERTIES), info);
        UnitTestUtil.helpTestEquivalence(1, new ServerConnectionInfo(BASE_PORTABLE_STRING), info);
        
        info.setTrustedPayload(null);
        UnitTestUtil.helpTestEquivalence(0, new ServerConnectionInfo(BASE_PORTABLE_STRING+ENCODED_OPTIONAL_PROPERTIES), info);
        UnitTestUtil.helpTestEquivalence(1, new ServerConnectionInfo(BASE_PORTABLE_STRING), info);
    }
    
    public void testGetPortableString() {
        ServerConnectionInfo info = new ServerConnectionInfo();
        info.setServerUrl("mm://myhost:31000"); //$NON-NLS-1$
        info.setUser("metamatrixadmin"); //$NON-NLS-1$
        info.setPassword("password"); //$NON-NLS-1$
        info.setVDBName("VDB"); //$NON-NLS-1$
        info.setVDBVersion("3"); //$NON-NLS-1$
        
        assertEquals(BASE_PORTABLE_STRING, info.getPortableString());
        
        info.setVDBVersion(null);
        assertEquals(BASE_PORTABLE_STRING_NO_VERSION, info.getPortableString());
        info.setVDBVersion("3"); //$NON-NLS-1$
        
        info.setTrustedPayload("trustedPayload"); //$NON-NLS-1$
        assertEquals(BASE_PORTABLE_STRING + ENCODED_TRUSTED_PAYLOAD, info.getPortableString());
        
        info.setOptionalProperty("optionalProperty1", new Integer(42)); //$NON-NLS-1$
        assertEquals(BASE_PORTABLE_STRING+ENCODED_TRUSTED_PAYLOAD+ENCODED_OPTIONAL_PROPERTIES, info.getPortableString());
        
        info.setTrustedPayload(null);
        assertEquals(BASE_PORTABLE_STRING+ENCODED_OPTIONAL_PROPERTIES, info.getPortableString());
    }

}
