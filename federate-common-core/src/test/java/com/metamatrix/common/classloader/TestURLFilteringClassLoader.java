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

import java.net.URL;
import java.util.HashSet;

import junit.framework.TestCase;

import com.metamatrix.common.protocol.MetaMatrixURLStreamHandlerFactory;
import com.metamatrix.common.protocol.URLHelper;


/** 
 * @since 4.3
 */
public class TestURLFilteringClassLoader extends TestCase {

    /*
     * Test method for 'com.metamatrix.common.classloader.URLFilteringClassLoader.getURLs()'
     */
    public void testGetURLs() throws Exception{
        URL http = URLHelper.buildURL("http://foo.com/foo.jar"); //$NON-NLS-1$
        URL ftp = URLHelper.buildURL("ftp://foo.com/foo.jar"); //$NON-NLS-1$
        URL mmfile = URLHelper.buildURL("mmfile:foo.jar"); //$NON-NLS-1$
        URL mmrofile = URLHelper.buildURL("mmrofile:foo.jar"); //$NON-NLS-1$
        URL classpath = URLHelper.buildURL("classpath:foo.jar"); //$NON-NLS-1$
        //we no longer need a jar of jars
        //URL jar = URLHelper.buildURL("jar:foo.jar"); //$NON-NLS-1$
        
        URL[] urls = { http, ftp, mmfile, mmrofile, classpath};
        URLFilteringClassLoader loader = new URLFilteringClassLoader(urls, this.getClass().getClassLoader(), new MetaMatrixURLStreamHandlerFactory());
        
        URL[] filtered = loader.getURLs();
        
        assertEquals(2, filtered.length);
        
        HashSet filteredURLSet = new HashSet();
        for( int i = 0; i < filtered.length; i++) {
            filteredURLSet.add(filtered[i]);
        }
        
        assertTrue(filteredURLSet.contains(http));
        assertTrue(filteredURLSet.contains(ftp));
        assertTrue(!filteredURLSet.contains(mmfile));
        assertTrue(!filteredURLSet.contains(mmrofile));
        assertTrue(!filteredURLSet.contains(classpath));
    }
}
