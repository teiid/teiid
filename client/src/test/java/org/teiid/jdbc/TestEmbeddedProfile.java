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

import static org.junit.Assert.*;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;
import org.teiid.jdbc.JDBCURL.ConnectionType;

public class TestEmbeddedProfile {
    
    /*
     * Test method for 'com.metamatrix.jdbc.EmbeddedEmbeddedProfile.acceptsURL(String)'
     * // (\\w:[\\\\,\\/]|file:\\/\\/|\\/|\\\\|(\\.){1,2}){1}
     */
    @Test public void testAcceptsURL() {    
        
        // ClassPath based URL
        assertFalse(ConnectionType.Embedded == JDBCURL.acceptsUrl("jdbc:teiid:BQT@classpath:/dqp.properties;partialResultsMode=true")); //$NON-NLS-1$
        
        assertEquals(ConnectionType.Embedded, JDBCURL.acceptsUrl("jdbc:teiid:BQT")); //$NON-NLS-1$
        assertNull(JDBCURL.acceptsUrl("jdbc:teiid:BQT!/path/foo.properties")); //$NON-NLS-1$
        assertEquals(ConnectionType.Embedded, JDBCURL.acceptsUrl("jdbc:teiid:BQT;")); //$NON-NLS-1$
        assertEquals(ConnectionType.Embedded, JDBCURL.acceptsUrl("jdbc:teiid:BQT;version=1;logFile=foo.txt")); //$NON-NLS-1$
        assertEquals(ConnectionType.Embedded, JDBCURL.acceptsUrl("jdbc:teiid:BQT.1;version=1;logFile=foo.txt")); //$NON-NLS-1$
    }

    @Test public void testParseURL() throws SQLException{
        Properties p = new Properties();
        TeiidDriver.parseURL("jdbc:teiid:BQT", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertEquals(2, p.size());        
    }

    @Test public void testParseURL2() throws SQLException {
        Properties p = new Properties();       
        TeiidDriver.parseURL("jdbc:teiid:BQT;version=3", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("3")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VERSION).equals("3")); //$NON-NLS-1$
        assertEquals(4, p.size());
    }
    
    @Test public void testParseURL3() throws SQLException{
        Properties p = new Properties();
        TeiidDriver.parseURL("jdbc:teiid:BQT;version=4;autoCommitTxn=ON;partialResultsMode=YES;", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("4")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VERSION).equals("4")); //$NON-NLS-1$
        assertTrue(p.getProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP).equals("ON")); //$NON-NLS-1$
        assertTrue(p.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE).equals("YES")); //$NON-NLS-1$
        assertEquals(6, p.size());        
    }
    
    @Test public void testParseURL4() throws SQLException{
        Properties p = new Properties();
        TeiidDriver.parseURL("jdbc:teiid:BQT;partialResultsMode=true", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE).equals("true")); //$NON-NLS-1$
        assertEquals(3, p.size());                
    }
    
    @Test public void testParseURL5() throws SQLException{
        Properties p = new Properties();
        TeiidDriver.parseURL("jdbc:teiid:BQT", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
    }
    
    @Test public void testParseURL55() throws SQLException{
        Properties p = new Properties();
        TeiidDriver.parseURL("jdbc:teiid:BQT;", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
    }    
       
    @Test public void testParseURL6() throws SQLException{
        Properties p = new Properties();
        TeiidDriver.parseURL("jdbc:teiid:BQT;partialResultsMode=true;version=1", p); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_NAME).equals("BQT")); //$NON-NLS-1$
        assertTrue(p.getProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE).equals("true")); //$NON-NLS-1$
        assertTrue(p.getProperty(BaseDataSource.VDB_VERSION).equals("1")); //$NON-NLS-1$
        assertEquals(5, p.size());                
        
    }
    
    @Test public void test() throws Exception {
        try {
            Class.forName("org.teiid.jdbc.TeiidDriver"); //$NON-NLS-1$
            DriverManager.getConnection("jdbc:teiid:Parts@invalidConfig.properties;version=1"); //$NON-NLS-1$
            fail();
        } catch (SQLException e) {
        }
    }
}
