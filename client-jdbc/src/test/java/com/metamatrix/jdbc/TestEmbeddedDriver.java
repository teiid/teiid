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

package com.metamatrix.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestEmbeddedDriver extends TestCase {

    EmbeddedDriver driver = new EmbeddedDriver();
    
    /*
     * Test method for 'com.metamatrix.jdbc.EmbeddedDriver.acceptsURL(String)'
     * // (\\w:[\\\\,\\/]|file:\\/\\/|\\/|\\\\|(\\.){1,2}){1}
     */
    public void testAcceptsURL() throws SQLException {    
        // Windows Path
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@c:\\metamatrix\\dqp\\dqp.properties")); //$NON-NLS-1$
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@c:\\metamatrix\\dqp\\dqp.properties;version=1")); //$NON-NLS-1$
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@c:\\metamatrix\\dqp\\dqp.properties;version=1;txnAutoWrap=ON;partialResultsMode=YES")); //$NON-NLS-1$
        
        // Alternative windows path
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@c:/metamatrix/dqp/dqp.properties")); //$NON-NLS-1$
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@c:/metamatrix/dqp/dqp.properties;version=1")); //$NON-NLS-1$
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@c:/metamatrix/dqp/dqp.properties;version=1;txnAutoWrap=ON;partialResultsMode=YES")); //$NON-NLS-1$

        // Abosolute path (Unix or windows)
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@/metamatrix/dqp/dqp.properties")); //$NON-NLS-1$
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@/metamatrix/dqp/dqp.properties;version=1")); //$NON-NLS-1$
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@/metamatrix/dqp/dqp.properties;version=1;txnAutoWrap=ON;partialResultsMode=YES")); //$NON-NLS-1$

        // relative path
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@../../metamatrix/dqp/dqp.properties")); //$NON-NLS-1$
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@../../metamatrix/dqp/dqp.properties;version=1")); //$NON-NLS-1$
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@../../metamatrix/dqp/dqp.properties;version=1;txnAutoWrap=ON;partialResultsMode=YES")); //$NON-NLS-1$
        
        // File URL should be supported (not sure)
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@file:///c:/metamatrix/dqp/dqp.properties")); //$NON-NLS-1$
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@testdata/dqp/dqp.properties;partialResultsMode=true")); //$NON-NLS-1$
        
        // ClassPath based URL
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@classpath:/dqp.properties;partialResultsMode=true")); //$NON-NLS-1$
        
        // These are specific to the MMDriver and should not be suported
        assertFalse(driver.acceptsURL("jdbc:metamatrix:BQT@mm://host:7001;version=1")); //$NON-NLS-1$
        assertFalse(driver.acceptsURL("jdbc:metamatrix:BQT@mms://host:7001;version=1")); //$NON-NLS-1$
        //assertFalse(driver.acceptsURL("jdbc:metamatrix:BQT@http://host:7001;version=1"));
        
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT")); //$NON-NLS-1$
        assertFalse(driver.acceptsURL("jdbc:metamatrix:BQT!/path/foo.properties")); //$NON-NLS-1$
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT;")); //$NON-NLS-1$
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT;version=1;logFile=foo.txt")); //$NON-NLS-1$
    }

    public void test() throws Exception {
        try {
            Class.forName("com.metamatrix.jdbc.EmbeddedDriver"); //$NON-NLS-1$
            DriverManager.getConnection("jdbc:metamatrix:Parts@invalidConfig.properties;version=1"); //$NON-NLS-1$
            fail();
        } catch (SQLException e) {
        }
    }
    
}
