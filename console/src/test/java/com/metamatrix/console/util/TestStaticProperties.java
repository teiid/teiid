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

package com.metamatrix.console.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.toolbox.preference.UserPreferences;


/** 
 * Tests <code>FileCache</code>
 * @since 4.2
 */
public class TestStaticProperties extends TestCase {

    private File file = null;
    
    
    public TestStaticProperties(String name) {
        super(name);
    }
    
    public void setUp() throws Exception {
    	System.setProperty("metamatrix.config.none", "none");
        file = File.createTempFile("test-static", "properties");
        
        
        Properties properties = new Properties();
        properties.setProperty(UserPreferences.USER_PREFERENCES_DEFINITION_FILE_PROPERTY_NAME,
            file.getAbsolutePath());
        UserPreferences.getInstance().setConfigProperties(properties);
    }
    
    public void tearDown() {
        file.delete();
    }
    
   
    /**
     * Set URLs, test that they are set. 
     * @throws Exception
     */
    public void testSetURLs1() throws Exception {
        List urls = new ArrayList();
        urls.add("url2");
        urls.add("url1");
        urls.add("url3");
        
        StaticProperties.setURLs(urls);
        StaticProperties.saveProperties();
        
        List results = StaticProperties.getURLsCopy();
        assertEquals("url1", results.get(0));
        assertEquals("url2", results.get(1));
        assertEquals("url3", results.get(2));
        
    }

    /**
     * Set URLs, test that they are set.
     * @throws Exception
     */
    public void testSetURLs2DontUseLast() throws Exception {
        List urls = new ArrayList();
        urls.add("url2");
        urls.add("url1");
        urls.add("url3");
        
        StaticProperties.setURLs(urls, "url4", false);
        StaticProperties.saveProperties();
        
        List results = StaticProperties.getURLsCopy();
        assertEquals("url1", results.get(0));
        assertEquals("url2", results.get(1));
        assertEquals("url3", results.get(2));
        
        assertEquals("url4", StaticProperties.getDefaultURL());
        assertFalse(StaticProperties.getUseLastURLAsDefault());
        
    }
    
    
    /**
     * Set some URLs, and then remove them.
     * Make sure that they are removed.
     * @throws Exception
     */
    public void testSetURLs2Remove() throws Exception {
        List urls = new ArrayList();
        urls.add("url2");
        urls.add("url1");
        urls.add("url3");
        urls.add("url4");
        
        StaticProperties.setURLs(urls);
        StaticProperties.saveProperties();
        
        
        urls = new ArrayList();
        urls.add("url4");
        urls.add("url1");
        
        StaticProperties.setURLs(urls);
        StaticProperties.saveProperties();
        
        List results = StaticProperties.getURLsCopy();
        assertEquals("url1", results.get(0));
        assertEquals("url4", results.get(1));
    }
}
