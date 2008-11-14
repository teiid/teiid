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

package com.metamatrix.common.classloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import junit.framework.TestCase;

import com.metamatrix.common.protocol.MetaMatrixURLStreamHandlerFactory;
import com.metamatrix.core.util.UnitTestUtil;

/**
 * Test {@link NonDelegatingClassLoader}
 */
public class TestNonDelegatingClassLoader extends TestCase {

    /**
     * @param name
     */
    public TestNonDelegatingClassLoader(String name) {
        super(name);
    }
    
    
    // ################################## HELPERS ##############################

    private URL[] getURLs() throws Exception {
    	URL[] result = new URL[2];
        result[0] = UnitTestUtil.getTestDataFile("extensionmodule/testjar.jar").toURL(); //$NON-NLS-1$
        result[1] = UnitTestUtil.getTestDataFile("extensionmodule/testjar2.jar").toURL(); //$NON-NLS-1$
        return result;
    }

    // ################################## TESTS ################################

    public void testLoadClass() throws Exception {
        String classname = "com.test.TestClass"; //$NON-NLS-1$
        String expectedToString = "This is a test class"; //$NON-NLS-1$

        NonDelegatingClassLoader loader = new NonDelegatingClassLoader(getURLs());

        Class clazz = loader.loadClass(classname);
        Object obj = clazz.newInstance();
        assertEquals(expectedToString, obj.toString());
    }

    public void testLoadResource() throws Exception {
        String resourcename = "com/test/TestDoc.txt"; //$NON-NLS-1$
        NonDelegatingClassLoader loader = new NonDelegatingClassLoader(getURLs());

        InputStream stream = loader.getResourceAsStream(resourcename);
        assertNotNull(stream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String text = reader.readLine();
        assertEquals("This is a test of extension module classloader.", text); //$NON-NLS-1$
    }

    /**
     * Tests that the classloader finds a file with the given name at the file URL BEFORE it finds the file through the system
     * classpath (i.e. test data directory). Two files with the same name but different contents are located at the two locations.
     * 
     * @throws Exception
     */
    public void testLoadResourceFileProtocol() throws Exception {
        String resourcename = "nonModelFile.txt"; //$NON-NLS-1$
        URL url = new URL("file", "localhost", UnitTestUtil.getTestDataPath()); //$NON-NLS-1$ //$NON-NLS-2$
        NonDelegatingClassLoader loader = new NonDelegatingClassLoader(new URL[] {
            url
        });

        InputStream stream = loader.getResourceAsStream(resourcename);
        assertNotNull(stream);
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String text = reader.readLine();
        assertEquals("This is a simple text document.", text); //$NON-NLS-1$
    }

    public void testGetURLs() throws Exception {
        URL url1 = new URL("extensionjar", "localhost", -1, "testjar.jar", new URLStreamHandler() {
			@Override
			protected URLConnection openConnection(URL u) throws IOException {
				return null;
			}}); //$NON-NLS-1$
        URL url2 = UnitTestUtil.getTestDataFile("Books.xsd").toURL(); //$NON-NLS-1$
        NonDelegatingClassLoader loader = new URLFilteringClassLoader(new URL[] {
            url1, url2
        }, this.getClass().getClassLoader(), new MetaMatrixURLStreamHandlerFactory());
        URL[] urls = loader.getURLs();
        assertNotNull(urls);
        assertEquals(1, urls.length);
        assertEquals(url2, urls[0]);
    }

}
