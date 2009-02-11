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

package com.metamatrix.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.metamatrix.jdbc.api.ExecutionProperties;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestEmbeddedDriver extends TestCase {

    EmbeddedDriver driver = new EmbeddedDriver();
    
    public void testGetVersion() {
        assertEquals(EmbeddedDriver.MAJOR_VERSION, driver.getMajorVersion());
        assertEquals(EmbeddedDriver.MINOR_VERSION, driver.getMinorVersion());
        assertEquals(EmbeddedDriver.DRIVER_NAME, driver.getDriverName());
    }
    
    /*
     * Test method for 'com.metamatrix.jdbc.EmbeddedDriver.acceptsURL(String)'
     * // (\\w:[\\\\,\\/]|file:\\/\\/|\\/|\\\\|(\\.){1,2}){1}
     */
    public void testAcceptsURL() throws SQLException {    
        // Windows Path
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@c:\\metamatrix\\dqp\\dqp.properties"));
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@c:\\metamatrix\\dqp\\dqp.properties;version=1"));
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@c:\\metamatrix\\dqp\\dqp.properties;version=1;txnAutoWrap=ON;partialResultsMode=YES"));
        
        // Alternative windows path
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@c:/metamatrix/dqp/dqp.properties"));
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@c:/metamatrix/dqp/dqp.properties;version=1"));
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@c:/metamatrix/dqp/dqp.properties;version=1;txnAutoWrap=ON;partialResultsMode=YES"));

        // Abosolute path (Unix or windows)
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@/metamatrix/dqp/dqp.properties"));
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@/metamatrix/dqp/dqp.properties;version=1"));
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@/metamatrix/dqp/dqp.properties;version=1;txnAutoWrap=ON;partialResultsMode=YES"));

        // relative path
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@../../metamatrix/dqp/dqp.properties"));
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@../../metamatrix/dqp/dqp.properties;version=1"));
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@../../metamatrix/dqp/dqp.properties;version=1;txnAutoWrap=ON;partialResultsMode=YES"));
        
        // File URL should be supported (not sure)
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@file:///c:/metamatrix/dqp/dqp.properties"));
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@testdata/dqp/dqp.properties;partialResultsMode=true"));
        
        // ClassPath based URL
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT@classpath:/dqp.properties;partialResultsMode=true"));
        
        // These are specific to the MMDriver and should not be suported
        assertFalse(driver.acceptsURL("jdbc:metamatrix:BQT@mm://host:7001;version=1"));
        assertFalse(driver.acceptsURL("jdbc:metamatrix:BQT@mms://host:7001;version=1"));
        //assertFalse(driver.acceptsURL("jdbc:metamatrix:BQT@http://host:7001;version=1"));
        
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT"));
        assertFalse(driver.acceptsURL("jdbc:metamatrix:BQT!/path/foo.properties"));
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT;"));
        assertTrue(driver.acceptsURL("jdbc:metamatrix:BQT;version=1;logFile=foo.txt"));
    }

    public void testParseURL() throws SQLException{
        Properties p = new Properties();
        driver.parseURL("jdbc:metamatrix:BQT@c:\\metamatrix\\dqp\\dqp.properties", p);
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT"));
        assertTrue(p.get(EmbeddedDataSource.DQP_BOOTSTRAP_FILE).toString().equals("mmfile:/c:/metamatrix/dqp/dqp.properties"));
        assertEquals(2, p.size());        
    }

    public void testParseURL2() throws SQLException {
        Properties p = new Properties();       
        driver.parseURL("jdbc:metamatrix:BQT@\\metamatrix\\dqp\\dqp.properties;version=3", p);
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT"));
        assertTrue(p.get(EmbeddedDataSource.DQP_BOOTSTRAP_FILE).toString().equals("mmfile:/metamatrix/dqp/dqp.properties"));
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("3"));
        assertTrue(p.getProperty(BaseDataSource.VERSION).equals("3"));
        assertEquals(4, p.size());
    }
    
    public void testParseURL3() throws SQLException{
        Properties p = new Properties();
        driver.parseURL("jdbc:metamatrix:BQT@/metamatrix/dqp/dqp.properties;version=4;txnAutoWrap=ON;partialResultsMode=YES;logFile=D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log", p);
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT"));
        assertTrue(p.get(EmbeddedDataSource.DQP_BOOTSTRAP_FILE).toString().equals("mmfile:/metamatrix/dqp/dqp.properties"));
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("4"));
        assertTrue(p.getProperty(BaseDataSource.VERSION).equals("4"));
        assertTrue(p.getProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP).equals("ON"));
        assertTrue(p.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE).equals("YES"));
        assertTrue(p.getProperty(BaseDataSource.LOG_FILE).equals("D:\\metamatrix\\work\\DQP\\log\\jdbcLogFile.log"));
        assertEquals(7, p.size());        
    }
    
    public void testParseURL4() throws SQLException{
        Properties p = new Properties();
        driver.parseURL("jdbc:metamatrix:BQT@testdata/dqp/dqp.properties;partialResultsMode=true", p);
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT"));
        assertTrue(p.get(EmbeddedDataSource.DQP_BOOTSTRAP_FILE).toString().equals("mmfile:testdata/dqp/dqp.properties"));
        assertTrue(p.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE).equals("true"));
        assertEquals(3, p.size());                
    }
    
    public void testParseURL5() throws SQLException{
        Properties p = new Properties();
        driver.parseURL("jdbc:metamatrix:BQT", p);
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT"));
        assertTrue(p.get(EmbeddedDataSource.DQP_BOOTSTRAP_FILE).toString().equals("classpath:/mm.properties"));        
    }
    
    public void testParseURL55() throws SQLException{
        Properties p = new Properties();
        driver.parseURL("jdbc:metamatrix:BQT;", p);
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT"));
        assertTrue(p.get(EmbeddedDataSource.DQP_BOOTSTRAP_FILE).toString().equals("classpath:/mm.properties"));        
    }    
       
    public void testParseURL6() throws SQLException{
        Properties p = new Properties();
        driver.parseURL("jdbc:metamatrix:BQT;partialResultsMode=true;version=1", p);
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT"));
        assertTrue(p.get(EmbeddedDataSource.DQP_BOOTSTRAP_FILE).toString().equals("classpath:/mm.properties"));
        assertTrue(p.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE).equals("true"));
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("1"));
        assertTrue(p.getProperty("vdb.definition").equals("BQT.vdb"));
        assertEquals(6, p.size());                
        
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
