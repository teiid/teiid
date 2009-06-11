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

package com.metamatrix.common.protocol;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Test;

import com.metamatrix.core.util.UnitTestUtil;


/** 
 * @since 4.3
 */
public class TestURLHelper {
    
    private URL url;
    
    private URL getUrl() throws MalformedURLException {
        if (url == null) {
            url = new URL("file", "localhost", System.getProperty("java.io.tmpdir").replace('\\', '/'));  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        return url;
    }
    
    @Test public void testBuildURL() throws Exception {
        assertEquals(new URL("http://metamatrix.com/foo.txt"), URLHelper.buildURL("http://metamatrix.com/foo.txt")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(new URL("file:/c:/metamatrix/foo.txt"), URLHelper.buildURL("file:/c:/metamatrix/foo.txt")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals(new URL("mmfile", "", -1, "/metamatrix/foo.txt"), URLHelper.buildURL("/metamatrix/foo.txt")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        assertEquals(new URL("mmfile", "", -1, "metamatrix/foo.txt"), URLHelper.buildURL("metamatrix/foo.txt")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        assertEquals(new URL("mmfile", "", -1, "/e:/metamatrix/foo.txt"), URLHelper.buildURL("e:\\metamatrix\\foo.txt")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        assertEquals(new URL("mmfile", "", -1, "e:metamatrix/foo.txt"), URLHelper.buildURL("e:metamatrix\\foo.txt")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        assertEquals(new URL("mmfile", "", -1, "/metamatrix/foo.txt"), URLHelper.buildURL("\\metamatrix\\foo.txt")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        
        assertEquals(new URL("classpath", "", -1, "/metamatrix/foo.txt"), URLHelper.buildURL("classpath:/metamatrix/foo.txt")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        assertEquals(new URL("classpath", "", -1, "metamatrix/foo.txt"), URLHelper.buildURL("classpath:metamatrix/foo.txt")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$        
    }

    @Test public void testBuildURLWithContext() throws Exception{

        URL context = new URL("http://metamatrix.com/bar.txt"); //$NON-NLS-1$
        
        assertEquals("http://mm.com/footoo.txt", URLHelper.buildURL(context, "http://mm.com/footoo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("http://metamatrix.com/foo.txt", URLHelper.buildURL(context, "/foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        
        context = new URL("file:/c:/metamatrix/bar.txt"); //$NON-NLS-1$
        assertEquals("file:/foo.txt", URLHelper.buildURL(context, "/foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("file:/d:/foo.txt", URLHelper.buildURL(context, "file:/d:/foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$        
        assertEquals("file:/c:/metamatrix/foo.txt", URLHelper.buildURL(context, "foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("file:/c:/metamatrix/foo.txt", URLHelper.buildURL(context, "./foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("file:/c:/foo.txt", URLHelper.buildURL(context, "../foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$

        context = new URL("mmfile:/metamatrix/bar.txt"); //$NON-NLS-1$
        assertEquals("mmfile:/foo.txt", URLHelper.buildURL(context, "/foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("mmfile:/metamatrix/foo.txt", URLHelper.buildURL(context, "foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("mmfile:/metamatrix/foo.txt", URLHelper.buildURL(context, "./foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("mmfile:/foo.txt", URLHelper.buildURL(context, "../foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        
        context = new URL("mmfile:metamatrix/bar.txt"); //$NON-NLS-1$
        assertEquals("mmfile:/foo.txt", URLHelper.buildURL(context, "/foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("mmfile:metamatrix/foo.txt", URLHelper.buildURL(context, "foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("mmfile:metamatrix/foo.txt", URLHelper.buildURL(context, "./foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$

        // This is bad needs to avoid this form in URLS
        assertEquals("mmfile:metamatrix/../foo.txt", URLHelper.buildURL(context, "../foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$

        context = new URL("mmfile:/c:/metamatrix/bar.txt"); //$NON-NLS-1$
        assertEquals("mmfile:/foo.txt", URLHelper.buildURL(context, "/foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("mmfile:/d:/foo.txt", URLHelper.buildURL(context, "mmfile:/d:/foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$        
        assertEquals("mmfile:/c:/metamatrix/foo.txt", URLHelper.buildURL(context, "foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("mmfile:/c:/metamatrix/foo.txt", URLHelper.buildURL(context, "./foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("mmfile:/c:/foo.txt", URLHelper.buildURL(context, "../foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
                
        context = new URL("classpath:/metamatrix/bar.txt"); //$NON-NLS-1$
        assertEquals("classpath:/foo.txt", URLHelper.buildURL(context, "/foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("classpath:/metamatrix/foo.txt", URLHelper.buildURL(context, "foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("classpath:/metamatrix/foo.txt", URLHelper.buildURL(context, "./foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("classpath:/foo.txt", URLHelper.buildURL(context, "../foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Note - this converts from mmrofile to mmfile
     * @throws Exception
     */
    @Test public void testMMROFileContext() throws Exception {
        URL context = URLHelper.buildURL("mmrofile:/metamatrix/bar.txt"); //$NON-NLS-1$
        assertEquals("mmfile:/d:/foo.txt", URLHelper.buildURL(context, "d:/foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("mmfile:/foo.txt?xyz=m", URLHelper.buildURL(context, "/foo.txt?xyz=m").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("mmfile:/metamatrix/foo.txt", URLHelper.buildURL(context, "foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("mmfile:/metamatrix/foo.txt", URLHelper.buildURL(context, "./foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("mmfile:/foo.txt", URLHelper.buildURL(context, "../foo.txt").toString()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testCreateFileFromUrl() throws Exception {
    	
    	// Test for creating File object for valid Http url 
		File file = URLHelper.createFileFromUrl(getUrl(),"wsdlFile",".wsdl"); //$NON-NLS-1$ //$NON-NLS-2$
    	assertNotNull(file);
		
    	// Test for creating File object for invalid (unavailable) url 
    	try {
			file = URLHelper.createFileFromUrl(UnitTestUtil.getTestDataFile("foo").toURI().toURL(),"wsdlFile",".wsdl"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			fail("expected exception");
		} catch (IOException e) {
		}
		
		// Test for creating File object for Http url with invalid protocol 
    	try {
			file = URLHelper.createFileFromUrl(new URL("xthp://metamatrix.netcom/"),"wsdlFile",".wsdl"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			fail("expected exception");
		} catch (MalformedURLException e) {
			assertEquals("unknown protocol: xthp", e.getMessage()); //$NON-NLS-1$
		}
    }
    
    @Test public void testCreateFileFromUrl2() throws Exception {
        
        // Test for creating File object for valid Http url 
        File file = URLHelper.createFileFromUrl(getUrl(),"xsdFile.xsd",null,null, true); //$NON-NLS-1$ 
        assertNotNull(file);
        
        // Test for creating File object for invalid (unavailable) url 
        try {
            file = URLHelper.createFileFromUrl(UnitTestUtil.getTestDataFile("foo").toURI().toURL(),"xsdFile.xsd",null,null, true); //$NON-NLS-1$ //$NON-NLS-2$
            fail("expected exception");
        } catch (IOException e) {
        }
        
        // Test for creating File object for Http url with invalid protocol 
        try {
            file = URLHelper.createFileFromUrl(new URL("xthp://metamatrix.netcom/"),"xsdFile.xsd",null,null, true); //$NON-NLS-1$ //$NON-NLS-2$
            fail("expected exception");
        } catch (MalformedURLException e) {
            assertEquals("unknown protocol: xthp", e.getMessage()); //$NON-NLS-1$
        }
    }
    
    @Test public void testResolveURL() throws Exception{

    	// Test for valid HTTP URL
		URLHelper.resolveUrl(getUrl()); 
        
		//Test for invalid HTTP URL
        try {
			URLHelper.resolveUrl(new URL("http://localhost/xyz")); //$NON-NLS-1$
			fail("expected exception");
		} catch (IOException e) {
		}
        
		//Test for invalid File URL
        try {
			URLHelper.resolveUrl(new URL("file:/com.metamatrix.common/testdata/Books.dsx")); //$NON-NLS-1$
			fail("expected exception");
		} catch (IOException e) {
		}

    }

    @Test public void testResolveURL2() throws Exception{
        // Test for valid HTTP URL
        URLHelper.resolveUrl(getUrl(), null, null, true); 
        
        //Test for invalid HTTP URL
        try {
            URLHelper.resolveUrl(new URL("http://localhost/xyz"), null, null, true); //$NON-NLS-1$
        } catch (IOException e) {
        }
        
        //Test for invalid File URL
        try {
            URLHelper.resolveUrl(new URL("file:/com.metamatrix.common/testdata/Books.dsx"), null, null, true); //$NON-NLS-1$ 
        } catch (IOException e) {
        }
    }
}
