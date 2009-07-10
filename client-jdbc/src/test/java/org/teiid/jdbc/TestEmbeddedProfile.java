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
package org.teiid.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.jdbc.BaseDataSource;
import com.metamatrix.jdbc.api.ExecutionProperties;


public class TestEmbeddedProfile {
    EmbeddedProfile EmbeddedProfile = new EmbeddedProfile();
    
    /*
     * Test method for 'com.metamatrix.jdbc.EmbeddedEmbeddedProfile.acceptsURL(String)'
     * // (\\w:[\\\\,\\/]|file:\\/\\/|\\/|\\\\|(\\.){1,2}){1}
     */
    @Test public void testAcceptsURL() {    
//        // Windows Path
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@c:\\metamatrix\\dqp\\dqp.properties")); //$NON-NLS-1$
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@c:\\metamatrix\\dqp\\dqp.properties;version=1")); //$NON-NLS-1$
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@c:\\metamatrix\\dqp\\dqp.properties;version=1;txnAutoWrap=ON;partialResultsMode=YES")); //$NON-NLS-1$
        
        // Alternative windows path
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@c:/metamatrix/dqp/dqp.properties")); //$NON-NLS-1$
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@c:/metamatrix/dqp/dqp.properties;version=1")); //$NON-NLS-1$
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@c:/metamatrix/dqp/dqp.properties;version=1;txnAutoWrap=ON;partialResultsMode=YES")); //$NON-NLS-1$

        // Abosolute path (Unix or windows)
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@/metamatrix/dqp/dqp.properties")); //$NON-NLS-1$
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@/metamatrix/dqp/dqp.properties;version=1")); //$NON-NLS-1$
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@/metamatrix/dqp/dqp.properties;version=1;txnAutoWrap=ON;partialResultsMode=YES")); //$NON-NLS-1$

        // relative path
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@../../metamatrix/dqp/dqp.properties")); //$NON-NLS-1$
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@../../metamatrix/dqp/dqp.properties;version=1")); //$NON-NLS-1$
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@../../metamatrix/dqp/dqp.properties;version=1;txnAutoWrap=ON;partialResultsMode=YES")); //$NON-NLS-1$
        
        // File URL should be supported (not sure)
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@file:///c:/metamatrix/dqp/dqp.properties")); //$NON-NLS-1$
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@testdata/dqp/dqp.properties;partialResultsMode=true")); //$NON-NLS-1$
        
        // ClassPath based URL
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@classpath:/dqp.properties;partialResultsMode=true")); //$NON-NLS-1$
        
        // These are specific to the MMorg.teiid.jdbc.EmbeddedProfile and should not be suported
        assertFalse(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@mm://host:7001;version=1")); //$NON-NLS-1$
        assertFalse(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@mms://host:7001;version=1")); //$NON-NLS-1$
        //assertFalse(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT@http://host:7001;version=1"));
        
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT")); //$NON-NLS-1$
        assertFalse(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT!/path/foo.properties")); //$NON-NLS-1$
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT;")); //$NON-NLS-1$
        assertTrue(org.teiid.jdbc.EmbeddedProfile.acceptsURL("jdbc:metamatrix:BQT;version=1;logFile=foo.txt")); //$NON-NLS-1$
    }

    @Test public void testParseURL() throws SQLException{
        Properties p = new Properties();
        org.teiid.jdbc.EmbeddedProfile.parseURL("jdbc:metamatrix:BQT@c:\\metamatrix\\dqp\\dqp.properties", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertEquals("c:\\metamatrix\\dqp\\dqp.properties", p.getProperty(DQPEmbeddedProperties.DQP_BOOTSTRAP_FILE)); //$NON-NLS-1$
        assertEquals(3, p.size());        
    }

    @Test public void testParseURL2() throws SQLException {
        Properties p = new Properties();       
        org.teiid.jdbc.EmbeddedProfile.parseURL("jdbc:metamatrix:BQT@\\metamatrix\\dqp\\dqp.properties;version=3", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertEquals("\\metamatrix\\dqp\\dqp.properties", p.getProperty(DQPEmbeddedProperties.DQP_BOOTSTRAP_FILE)); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("3")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VERSION).equals("3")); //$NON-NLS-1$
        assertEquals(5, p.size());
    }
    
    @Test public void testParseURL3() throws SQLException{
        Properties p = new Properties();
        org.teiid.jdbc.EmbeddedProfile.parseURL("jdbc:metamatrix:BQT@/metamatrix/dqp/dqp.properties;version=4;txnAutoWrap=ON;partialResultsMode=YES;", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("4")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VERSION).equals("4")); //$NON-NLS-1$
        assertTrue(p.getProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP).equals("ON")); //$NON-NLS-1$
        assertTrue(p.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE).equals("YES")); //$NON-NLS-1$
        assertEquals(7, p.size());        
    }
    
    @Test public void testParseURL4() throws SQLException{
        Properties p = new Properties();
        org.teiid.jdbc.EmbeddedProfile.parseURL("jdbc:metamatrix:BQT@testdata/dqp/dqp.properties;partialResultsMode=true", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertEquals("testdata/dqp/dqp.properties", p.getProperty(DQPEmbeddedProperties.DQP_BOOTSTRAP_FILE)); //$NON-NLS-1$
        assertTrue(p.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE).equals("true")); //$NON-NLS-1$
        assertEquals(4, p.size());                
    }
    
    @Test public void testParseURL5() throws SQLException{
        Properties p = new Properties();
        org.teiid.jdbc.EmbeddedProfile.parseURL("jdbc:metamatrix:BQT", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.get(DQPEmbeddedProperties.DQP_BOOTSTRAP_FILE).equals("classpath:/deploy.properties"));         //$NON-NLS-1$
    }
    
    @Test public void testParseURL55() throws SQLException{
        Properties p = new Properties();
        org.teiid.jdbc.EmbeddedProfile.parseURL("jdbc:metamatrix:BQT;", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.get(DQPEmbeddedProperties.DQP_BOOTSTRAP_FILE).equals("classpath:/deploy.properties"));         //$NON-NLS-1$
    }    
       
    @Test public void testParseURL6() throws SQLException{
        Properties p = new Properties();
        org.teiid.jdbc.EmbeddedProfile.parseURL("jdbc:metamatrix:BQT;partialResultsMode=true;version=1", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.get(DQPEmbeddedProperties.DQP_BOOTSTRAP_FILE).equals("classpath:/deploy.properties")); //$NON-NLS-1$
        assertTrue(p.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE).equals("true")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("1")); //$NON-NLS-1$
        assertTrue(p.getProperty("vdb.definition").equals("BQT.vdb")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(7, p.size());                
        
    }
    
    @Test public void test() throws Exception {
        try {
            Class.forName("org.teiid.jdbc.TeiidDriver"); //$NON-NLS-1$
            DriverManager.getConnection("jdbc:metamatrix:Parts@invalidConfig.properties;version=1"); //$NON-NLS-1$
            fail();
        } catch (SQLException e) {
        }
    }
}
