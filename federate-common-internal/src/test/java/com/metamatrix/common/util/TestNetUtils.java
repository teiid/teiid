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

package com.metamatrix.common.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import junit.framework.TestCase;

/**
 * @since	  3.0
 * @version   3.0
 * @author	  Randall M. Hauch
 */
public class TestNetUtils extends TestCase {

    public TestNetUtils(String name) {
        super(name);
    }

    //  ********* H E L P E R   M E T H O D S  ********* 
    public void helpTestGetFilename(String uri, String expectedResult){
        String result = NetUtils.getFilename(uri);
        assertEquals("Unexpected getFilename result", expectedResult, result ); //$NON-NLS-1$
    }
    
    public void helpTestGetFilenameWithoutSuffix(String uri, String expectedResult){
        String result = NetUtils.getFilenameWithoutSuffix(uri);
        assertEquals("Unexpected getFilename result", expectedResult, result ); //$NON-NLS-1$
    }
    

    //  ********* T E S T   S U I T E   M E T H O D S  ********* 
    public void testGetFilename1() {
        helpTestGetFilename("testURI.html", "testURI.html"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilename2() {
        helpTestGetFilename("http://www.ncsa.uiuc.edu/demoweb/url-primer.html", "url-primer.html"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilename3() {
        helpTestGetFilename("//index.html", "index.html"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilename4() {
        helpTestGetFilename("testURI.html.extra", "testURI.html.extra"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilename5() {
        helpTestGetFilename("http://www.ncsa.uiuc.edu/demoweb/url-primer.html.extra", "url-primer.html.extra"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilename6() {
        helpTestGetFilename("//index.html.extra", "index.html.extra"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilename7() {
        helpTestGetFilename("index", "index"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilenameWithoutSuffix1() {
        helpTestGetFilenameWithoutSuffix("testURI.html", "testURI"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilenameWithoutSuffix2() {
        helpTestGetFilenameWithoutSuffix("http://www.ncsa.uiuc.edu/demoweb/url-primer.html", "url-primer"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilenameWithoutSuffix3() {
        helpTestGetFilenameWithoutSuffix("//index.html", "index"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilenameWithoutSuffix4() {
        helpTestGetFilenameWithoutSuffix("testURI.html.extra", "testURI"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilenameWithoutSuffix5() {
        helpTestGetFilenameWithoutSuffix("http://www.ncsa.uiuc.edu/demoweb/url-primer.html.extra", "url-primer"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilenameWithoutSuffix6() {
        helpTestGetFilenameWithoutSuffix("//index.html.extra", "index"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testGetFilenameWithoutSuffix7() {
        helpTestGetFilenameWithoutSuffix("index", "index"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testResolveHostName() {
        
        try {
            String hostname = NetUtils.getHostname();
            assertNotNull(hostname, "Host name is not resolved");//$NON-NLS-1$
        } catch (UnknownHostException err) {
            fail("Unable to get host name"); //$NON-NLS-1$
        }

    }
    
    public void testGetHostShortName() {
        
        String hostname;
        try {
            hostname = NetUtils.getHostShortName();
            assertNotNull(hostname, "Host short name is not resolved");//$NON-NLS-1$ 
            
        } catch (UnknownHostException err) {
            fail("Unable to get short host name");//$NON-NLS-1$ 
        }

    }    
    
    public void testGetFirstNonLoopBackAddress() {
        
        InetAddress hostname;
        try {
            hostname = NetUtils.getFirstNonLoopbackAddress();
            assertNotNull(hostname);
            
        } catch (Exception err) {
            fail("Unable to get first non loop back address");//$NON-NLS-1$ 
        }

    }      
    
}
