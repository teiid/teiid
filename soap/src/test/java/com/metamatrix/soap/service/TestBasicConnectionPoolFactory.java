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

package com.metamatrix.soap.service;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestBasicConnectionPoolFactory extends TestCase {
    
    private static final BasicConnectionPoolFactory factory = new BasicConnectionPoolFactory();
    
    private static final String TEST_PROPERTY_VALUE = "test"; //$NON-NLS-1$

    /*
     * Test method for 'com.metamatrix.soap.service.BasicConnectionPoolFactory.getProperty(String)'
     */
    public void testGetPropertyWSystemPropertySet() {
        System.setProperty(ConnectionPoolFactory.INITIAL_POOL_SIZE_PROPERTY_KEY, TEST_PROPERTY_VALUE);
        assertEquals(TEST_PROPERTY_VALUE, factory.getProperty(ConnectionPoolFactory.INITIAL_POOL_SIZE_PROPERTY_KEY));
    }
    
    public void testGetPropertyRetreiveDefault() {
        assertEquals(String.valueOf(BasicConnectionPoolFactory.MAX_ACTIVE_CONNECTIONS_DEFAULT), factory.getProperty(ConnectionPoolFactory.MAX_ACTIVE_CONNECTIONS_PROPERTY_KEY));
    }

}
