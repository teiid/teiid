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

package com.metamatrix.common.util;

import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;


/** 
 * @since 4.2
 */
public class TestMMURL extends TestCase {
    
    public static final String REQUIRED_URL = MMURL.FORMAT_SERVER;

    /**
     * Constructor for TestMMURL.
     * @param name
     */
    public TestMMURL(String name) {
        super(name);
    }
    
    public final void testMMURL() {
        String SERVER_URL = "mm://localhost:31000"; //$NON-NLS-1$
        assertTrue(MMURL.isValidServerURL(SERVER_URL)); 
        
        MMURL url = new MMURL(SERVER_URL);
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 1 Host", hosts );  //$NON-NLS-1$ 
        assertEquals(1,hosts.size());  
    }
    
    public final void testBogusProtocol() {  
        String SERVER_URL = "foo://localhost:31000"; //$NON-NLS-1$
        assertFalse(MMURL.isValidServerURL(SERVER_URL));
        
        try {
            new MMURL(SERVER_URL);
            fail("MM URL passed non standard protocal fine"); //$NON-NLS-1$
        } catch (RuntimeException e) {
            assertEquals(MMURL.INVALID_FORMAT_SERVER, e.getMessage());
        }
    }
    public final void testBogusProtocol1() {       
        String SERVER_URL = "foo://localhost:31000"; //$NON-NLS-1$
        assertFalse(MMURL.isValidServerURL(SERVER_URL));

        try {
            new MMURL(SERVER_URL);  
            fail("MM URL passed non standard protocal fine"); //$NON-NLS-1$
        } catch (RuntimeException e) {
            assertEquals(MMURL.INVALID_FORMAT_SERVER, e.getMessage());
        }
    }
    
    public final void testMMURLSecure() {
        String SERVER_URL = "mms://localhost:31000"; //$NON-NLS-1$
        assertTrue(MMURL.isValidServerURL(SERVER_URL)); 

        MMURL url = new MMURL(SERVER_URL); 
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 1 Host", hosts );  //$NON-NLS-1$ 
        assertEquals(1,hosts.size());  
    }
    
    public final void testMMURLBadProtocolMM() {
        String SERVER_URL = "mmm://localhost:31000"; //$NON-NLS-1$
        assertFalse(MMURL.isValidServerURL(SERVER_URL)); 

        try {
            new MMURL(SERVER_URL); 
            fail("MMURL did not throw an exception"); //$NON-NLS-1$
        } catch( IllegalArgumentException e ) {
            assertEquals(MMURL.INVALID_FORMAT_SERVER,e.getMessage());
        }
    }
    
    public final void testMMURLWrongSlash() {
        String SERVER_URL = "mm:\\\\localhost:31000"; //$NON-NLS-1$
        assertFalse(MMURL.isValidServerURL(SERVER_URL));

        try {
            new MMURL(SERVER_URL); 
            fail("MMURL did not throw an exception"); //$NON-NLS-1$
        } catch( IllegalArgumentException e ) {
            assertEquals(MMURL.INVALID_FORMAT_SERVER,e.getMessage());
        }
    }
    
    public final void testMMURLOneSlash() {
        String SERVER_URL = "mm:/localhost:31000"; //$NON-NLS-1$
        assertFalse(MMURL.isValidServerURL(SERVER_URL)); 
        
        try {
            new MMURL(SERVER_URL); 
            fail("MMURL did not throw an exception"); //$NON-NLS-1$
        } catch( IllegalArgumentException e ) {
            assertEquals(MMURL.INVALID_FORMAT_SERVER,e.getMessage());
        }
    }
    
    public final void testMMURLNoHost() {
        String SERVER_URL = "mm://:31000"; //$NON-NLS-1$
        assertFalse(MMURL.isValidServerURL(SERVER_URL)); 

        try {
            new MMURL(SERVER_URL); 
            fail("MMURL did not throw an exception"); //$NON-NLS-1$
        } catch( IllegalArgumentException e ) {
            assertEquals(MMURL.INVALID_FORMAT_SERVER,e.getMessage());
        }
    }
    
    public final void testMMURLNoHostAndPort() {
        String SERVER_URL = "mm://:"; //$NON-NLS-1$
        assertFalse(MMURL.isValidServerURL(SERVER_URL)); 

        try {
            new MMURL(SERVER_URL);
            fail("MMURL did not throw an exception"); //$NON-NLS-1$
        } catch( IllegalArgumentException e ) {
            assertEquals(MMURL.INVALID_FORMAT_SERVER,e.getMessage());
        }
    }
    
    public final void testMMURLNoHostAndPort2() {
        String SERVER_URL = "mm://"; //$NON-NLS-1$
        assertFalse(MMURL.isValidServerURL(SERVER_URL)); 

        try {
            new MMURL(SERVER_URL); 
            fail("MMURL did not throw an exception"); //$NON-NLS-1$
        } catch( IllegalArgumentException e ) {
            assertEquals(MMURL.INVALID_FORMAT_SERVER,e.getMessage());
        }
    }
    
    public final void testMMURLBadPort() {
        String SERVER_URL = "mm://localhost:port"; //$NON-NLS-1$
        assertFalse(MMURL.isValidServerURL(SERVER_URL)); 

        try {
            new MMURL(SERVER_URL); 
            fail("MMURL did not throw an Exception"); //$NON-NLS-1$
        } catch( IllegalArgumentException e ) {            
        }
    }
    
    public final void testMMURL2Hosts() {
        String SERVER_URL = "mm://localhost:31000,localhost:31001"; //$NON-NLS-1$        
        assertTrue(MMURL.isValidServerURL(SERVER_URL)); 

        MMURL url = new MMURL(SERVER_URL); 
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 2 Host", hosts );  //$NON-NLS-1$ 
        assertEquals(2,hosts.size());  
    }
    
    public final void testMMURL3Hosts() {
        String SERVER_URL = "mm://localhost:31000,localhost:31001,localhost:31002"; //$NON-NLS-1$        
        assertTrue(MMURL.isValidServerURL(SERVER_URL)); 

        MMURL url = new MMURL(SERVER_URL); 
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 3 Host", hosts );  //$NON-NLS-1$ 
        assertEquals(3,hosts.size());  
    }

    public final void testGetHostInfo() {
        String SERVER_URL = "mm://localhost:31000"; //$NON-NLS-1$        
        assertTrue(MMURL.isValidServerURL(SERVER_URL)); 
 
        MMURL url = new MMURL(SERVER_URL); 
        assertNotNull(url.getHostInfo() );  
    }

    public final void testGetProtocolStandalone() {
        MMURL url = new MMURL("mm://localhost:31000"); //$NON-NLS-1$
        assertNotNull(url);
        assertEquals("mm://localhost:31000",url.getAppServerURL()); //$NON-NLS-1$
    }

    public final void testHasMoreElements() {
        MMURL url = new MMURL("mm://localhost:31000,localhost:31001"); //$NON-NLS-1$
        assertNotNull(url);
        assertFalse(url.getHostInfo().isEmpty());
    }

    public final void testNextElement() {
        MMURL url = new MMURL("mm://localhost:31000,localhost:31001"); //$NON-NLS-1$
        assertEquals(2, url.getHostInfo().size());
    }

    public final void testHostInfoEquals() {
        HostInfo expectedResults = new HostInfo("localhost",31000);  //$NON-NLS-1$
        MMURL url = new MMURL("mm://localhost:31000"); //$NON-NLS-1$
        HostInfo actualResults = url.getHostInfo().get(0);
        assertEquals(expectedResults,actualResults);
    }
        
    public final void testWithEmbeddedSpaces() {
        HostInfo expectedResults = new HostInfo("localhost",12345);  //$NON-NLS-1$
        
        MMURL url = new MMURL("mm://localhost : 12345"); //$NON-NLS-1$
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 1 Host", hosts );  //$NON-NLS-1$ 
        assertEquals(1,hosts.size());  
        HostInfo actualResults = url.getHostInfo().get(0);
        assertEquals(expectedResults,actualResults);
    }
    
    public final void testHostPortConstructor() {
        HostInfo expectedResults = new HostInfo("myhost", 12345);  //$NON-NLS-1$
        
        MMURL url = new MMURL("myhost", 12345, false); //$NON-NLS-1$
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 1 Host", hosts );  //$NON-NLS-1$ 
        assertEquals(1,hosts.size());  
        HostInfo actualResults = url.getHostInfo().get(0);
        assertEquals(expectedResults,actualResults);
        assertEquals("mm://myhost:12345", url.getAppServerURL()); //$NON-NLS-1$
    }
    
    public final void testHostPortConstructorSSL() {
        HostInfo expectedResults = new HostInfo("myhost",12345);  //$NON-NLS-1$ 
        
        MMURL url = new MMURL("myhost", 12345, true); //$NON-NLS-1$
        List hosts = url.getHostInfo();
        assertNotNull("MMURL should have 1 Host", hosts );  //$NON-NLS-1$ 
        assertEquals(1,hosts.size());  
        HostInfo actualResults = url.getHostInfo().get(0);
        assertEquals(expectedResults,actualResults);
        assertEquals("mms://myhost:12345", url.getAppServerURL()); //$NON-NLS-1$
    }
    
}
