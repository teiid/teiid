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

package com.metamatrix.metadata.runtime.model;

import java.util.ArrayList;
import java.util.List;

import org.teiid.connector.metadata.runtime.MetadataConstants;

import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.metadata.runtime.api.Group;
import com.metamatrix.metadata.runtime.api.ModelID;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestMetadataCache extends TestCase {

    private MetadataCache cache;
    
    /**
     * Constructor for TestMetadataCache.
     * @param name
     */
    public TestMetadataCache(String name) {
        super(name);
    }

    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();

        System.setProperty("metamatrix.config.none", "true");
        
        cache = new MetadataCache();
        cache.init("testvdb", "1", "testvdb.vdb", ObjectConverterUtil.convertToByteArray(this.getClass().getClassLoader().getResourceAsStream("PartsSupplier.vdb")));
        //cache.loadModelDetails();
        
    }
    
    /**
     * Tests MetadataCache.buildGroupObjects for TABLE groups
     * @since 4.3
     */
    public void testBuildGroupObjects_TABLE() throws Exception {
        //add row for a TABLE
        List row1 = new ArrayList();
        row1.add("1");  //uuid
        row1.add("group1"); //name
        row1.add("model1.group1"); //fullName
        row1.add(new Integer(MetadataConstants.TABLE_TYPES.TABLE_TYPE)); //tableType
        row1.add(new Boolean(true)); //isPhysical
        row1.add(new Boolean(false)); //supportsUpdate
        row1.add("model1"); //modelName
        row1.add(new Boolean(false)); //isSystem
       
        //add to the cache 
        List rowList = new ArrayList();
        rowList.add(row1);
        cache.buildGroupObjects(rowList.iterator());
        
        
        //check the cache contents
        ModelID modelID = new BasicModelID("model1");
        List groups = cache.getGroups(modelID);
        
        assertEquals(1, groups.size());
        
        Group group1 = (Group) groups.get(0);
        assertEquals("group1", group1.getName());
        assertFalse(group1.supportsUpdate());    
        assertFalse(group1.isVirtualDocument());
        
    }
    
    
    /**
     * Tests MetadataCache.buildGroupObjects for VIEW groups
     * @since 4.3
     */
    public void testBuildGroupObjects_VIEW() throws Exception {
        //add row for a TABLE
        List row1 = new ArrayList();
        row1.add("1");  //uuid
        row1.add("group1"); //name
        row1.add("model1.group1"); //fullName
        row1.add(new Integer(MetadataConstants.TABLE_TYPES.VIEW_TYPE)); //tableType
        row1.add(new Boolean(true)); //isPhysical
        row1.add(new Boolean(false)); //supportsUpdate
        row1.add("model1"); //modelName
        row1.add(new Boolean(false)); //isSystem
       
        //add to the cache 
        List rowList = new ArrayList();
        rowList.add(row1);
        cache.buildGroupObjects(rowList.iterator());
        
        
        //check the cache contents
        ModelID modelID = new BasicModelID("model1");
        List groups = cache.getGroups(modelID);
        
        assertEquals(1, groups.size());
        
        Group group1 = (Group) groups.get(0);
        assertEquals("group1", group1.getName());
        assertFalse(group1.supportsUpdate());    
        assertFalse(group1.isVirtualDocument());
        
    }

    /**
     * Tests MetadataCache.buildGroupObjects for DOCUMENT groups
     * @since 4.3
     */
    public void testBuildGroupObjects_DOCUMENT() throws Exception {
        //add row for a TABLE
        List row1 = new ArrayList();
        row1.add("1");  //uuid
        row1.add("group1"); //name
        row1.add("model1.group1"); //fullName
        row1.add(new Integer(MetadataConstants.TABLE_TYPES.DOCUMENT_TYPE)); //tableType
        row1.add(new Boolean(true)); //isPhysical
        row1.add(new Boolean(false)); //supportsUpdate
        row1.add("model1"); //modelName
        row1.add(new Boolean(false)); //isSystem
       
        //add to the cache 
        List rowList = new ArrayList();
        rowList.add(row1);
        cache.buildGroupObjects(rowList.iterator());
        
        
        //check the cache contents
        ModelID modelID = new BasicModelID("model1");
        List groups = cache.getGroups(modelID);
        
        assertEquals(1, groups.size());
        
        Group group1 = (Group) groups.get(0);
        assertEquals("group1", group1.getName());
        assertFalse(group1.supportsUpdate());    
        assertTrue(group1.isVirtualDocument());
        
    }
    
    
    /**
     * Tests MetadataCache.buildGroupObjects for MATERIALIZED groups
     * @since 4.3
     */
    public void testBuildGroupObjects_MATERIALIZED() throws Exception {
        //add row for a TABLE
        List row1 = new ArrayList();
        row1.add("1");  //uuid
        row1.add("group1"); //name
        row1.add("model1.group1"); //fullName
        row1.add(new Integer(MetadataConstants.TABLE_TYPES.MATERIALIZED_TYPE)); //tableType
        row1.add(new Boolean(false)); //isPhysical
        row1.add(new Boolean(true)); //supportsUpdate
        row1.add("model1"); //modelName
        row1.add(new Boolean(false)); //isSystem
       
        //add to the cache 
        List rowList = new ArrayList();
        rowList.add(row1);
        cache.buildGroupObjects(rowList.iterator());
        
        
        //check the cache contents
        ModelID modelID = new BasicModelID("model1");
        List groups = cache.getGroups(modelID);
        
        assertEquals(1, groups.size());
        
        Group group1 = (Group) groups.get(0);
        assertEquals("group1", group1.getName());
        assertFalse(group1.isVirtualDocument());
        //materialed tables should ALWAYS support update
        assertTrue(group1.supportsUpdate());   
    }
      

    /**
     * Tests MetadataCache.buildGroupObjects for XML_STAGING_TABLE groups
     * @since 4.3
     */
    public void testBuildGroupObjects_XML_STAGING_TABLE() throws Exception {
        //add row for a TABLE
        List row1 = new ArrayList();
        row1.add("1");  //uuid
        row1.add("group1"); //name
        row1.add("model1.group1"); //fullName
        row1.add(new Integer(MetadataConstants.TABLE_TYPES.XML_STAGING_TABLE_TYPE)); //tableType
        row1.add(new Boolean(false)); //isPhysical
        row1.add(new Boolean(true)); //supportsUpdate
        row1.add("model1"); //modelName
        row1.add(new Boolean(false)); //isSystem
       
        //add to the cache 
        List rowList = new ArrayList();
        rowList.add(row1);
        cache.buildGroupObjects(rowList.iterator());
        
        
        //check the cache contents
        ModelID modelID = new BasicModelID("model1");
        List groups = cache.getGroups(modelID);
        
        //XML_STAGING_TABLES should not be added to the cache
        assertEquals(0, groups.size());
               
    }
    
    
    /**
     * Tests MetadataCache.buildGroupObjects for XML_MAPPING_CLASS groups
     * @since 4.3
     */
    public void testBuildGroupObjects_XML_MAPPING_CLASS() throws Exception {
        //add row for a TABLE
        List row1 = new ArrayList();
        row1.add("1");  //uuid
        row1.add("group1"); //name
        row1.add("model1.group1"); //fullName
        row1.add(new Integer(MetadataConstants.TABLE_TYPES.XML_MAPPING_CLASS_TYPE)); //tableType
        row1.add(new Boolean(false)); //isPhysical
        row1.add(new Boolean(true)); //supportsUpdate
        row1.add("model1"); //modelName
        row1.add(new Boolean(false)); //isSystem
       
        //add to the cache 
        List rowList = new ArrayList();
        rowList.add(row1);
        cache.buildGroupObjects(rowList.iterator());
        
        
        //check the cache contents
        ModelID modelID = new BasicModelID("model1");
        List groups = cache.getGroups(modelID);
        
        //XML_MAPPING_CLASSES should not be added to the cache
        assertEquals(0, groups.size());
               
    }
}
