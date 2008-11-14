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

/*
 */
package com.metamatrix.dqp.internal.datamgr.metadata;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataObject;
import com.metamatrix.query.unittest.FakeMetadataStore;

public class TestMetadataFactory  extends TestCase {
    private MetadataFactory metadataFactory;
    private FakeMetadataObject pm1g1;
    private FakeMetadataObject pm1g1e1;
    
    public TestMetadataFactory(String name) {
        super(name);
    }
    
    public void setUp(){
        FakeMetadataStore store = new FakeMetadataStore();
        pm1g1 = createGroup("pm1.g1", null); //$NON-NLS-1$
        List pm1g1e = createElements(pm1g1, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        pm1g1e1 = (FakeMetadataObject)pm1g1e.get(0);
        store.addObject(pm1g1);
        store.addObjects(pm1g1e);
        metadataFactory = new MetadataFactory(new FakeMetadataFacade(store));
    }


    public static FakeMetadataObject createGroup(String name, FakeMetadataObject model) {
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.GROUP);
        obj.putProperty(FakeMetadataObject.Props.MODEL, model);
        obj.putProperty(FakeMetadataObject.Props.IS_VIRTUAL, Boolean.FALSE);
        obj.putProperty(FakeMetadataObject.Props.UPDATE, Boolean.TRUE); 
        obj.putProperty(FakeMetadataObject.Props.TEMP, Boolean.FALSE);  
        obj.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, name.substring(name.lastIndexOf(".")+1)); //$NON-NLS-1$
        return obj; 
    }
    
    public static FakeMetadataObject createElement(String name, FakeMetadataObject group, String type, int index) { 
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.ELEMENT);
        obj.putProperty(FakeMetadataObject.Props.MODEL, group.getProperty(FakeMetadataObject.Props.MODEL));
        obj.putProperty(FakeMetadataObject.Props.GROUP, group);
        obj.putProperty(FakeMetadataObject.Props.TYPE, type);
        
        obj.putProperty(FakeMetadataObject.Props.SELECT, Boolean.TRUE); 
        if(type.equals("string")) {   //$NON-NLS-1$
            obj.putProperty(FakeMetadataObject.Props.SEARCHABLE_LIKE, Boolean.TRUE);        
        } else {
            obj.putProperty(FakeMetadataObject.Props.SEARCHABLE_LIKE, Boolean.FALSE);
        }   
        obj.putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, Boolean.TRUE);     
        obj.putProperty(FakeMetadataObject.Props.NULL, Boolean.TRUE);
        obj.putProperty(FakeMetadataObject.Props.AUTO_INCREMENT, Boolean.FALSE);
        obj.putProperty(FakeMetadataObject.Props.DEFAULT_VALUE, null);
        obj.putProperty(FakeMetadataObject.Props.INDEX, new Integer(index));
        obj.putProperty(FakeMetadataObject.Props.UPDATE, Boolean.TRUE);
        obj.putProperty(FakeMetadataObject.Props.LENGTH, "100"); //$NON-NLS-1$
        obj.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, name.substring(name.lastIndexOf(".")+1)); //$NON-NLS-1$
        return obj; 
    }
    
    public static List createElements(FakeMetadataObject group, String[] names, String[] types) { 
        String groupRoot = group.getName() + "."; //$NON-NLS-1$
        List elements = new ArrayList();
        
        for(int i=0; i<names.length; i++) { 
            FakeMetadataObject element = createElement(groupRoot + names[i], group, types[i], i);
            elements.add(element);      
        }
        
        return elements;
    }
    
    //tests
    
    public void testCreateMetadataID(){        
        try{        
            //test create MetadataID for Group
            MetadataIDImpl gID = (MetadataIDImpl)metadataFactory.createMetadataID(pm1g1, MetadataID.TYPE_GROUP);
            assertEquals(gID.getActualMetadataID(), pm1g1);
            assertEquals(((MetadataIDImpl)gID.getChildIDs().get(0)).getActualMetadataID(), pm1g1e1);
            
            //test create MetadataID for Element
            MetadataIDImpl eID = (MetadataIDImpl)metadataFactory.createMetadataID(pm1g1e1, MetadataID.TYPE_ELEMENT);
            assertEquals(eID.getActualMetadataID(), pm1g1e1);
            assertEquals(((MetadataIDImpl)eID.getParentID()).getActualMetadataID(), pm1g1);
        }catch(Exception e){
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
    
    public void testRuntimeMetadata(){
        try{
            RuntimeMetadata runtimeMetadata = metadataFactory.createRuntimeMetadata();
            MetadataIDImpl gID = (MetadataIDImpl)metadataFactory.createMetadataID(pm1g1, MetadataID.TYPE_GROUP);
            GroupImpl group = (GroupImpl)runtimeMetadata.getObject(gID);
            assertEquals(group.getNameInSource(), "g1"); //$NON-NLS-1$
            assertEquals(((MetadataIDImpl)group.getMetadataID()).getActualMetadataID(), pm1g1);

            MetadataIDImpl eID = (MetadataIDImpl)metadataFactory.createMetadataID(pm1g1e1, MetadataID.TYPE_ELEMENT);
            ElementImpl element = (ElementImpl)runtimeMetadata.getObject(eID);
            assertEquals(element.getLength(), 100);
            assertEquals(element.getJavaType(), DataTypeManager.DefaultDataClasses.STRING);
            assertEquals(element.getNameInSource(), "e1"); //$NON-NLS-1$
            assertEquals(((MetadataIDImpl)element.getMetadataID()).getActualMetadataID(), pm1g1e1);
        }catch(Exception e){
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
    
    public void testGetVDBResourcePaths() {
        String[] expectedPaths = new String[] {"my/resource/path"}; //$NON-NLS-1$
        try {
            RuntimeMetadata runtimeMetadata = metadataFactory.createRuntimeMetadata();
            String[] mfPaths = metadataFactory.getVDBResourcePaths();
            String[] rtmdPaths = runtimeMetadata.getVDBResourcePaths();
            assertEquals(expectedPaths.length, mfPaths.length);
            assertEquals(expectedPaths.length, rtmdPaths.length);
            for (int i = 0; i < expectedPaths.length; i++) {
                assertEquals(expectedPaths[i], mfPaths[i]);
                assertEquals(expectedPaths[i], rtmdPaths[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
     
    public void testGetBinaryVDBResource() {
        try {
            RuntimeMetadata runtimeMetadata = metadataFactory.createRuntimeMetadata();
            byte[] expectedBytes = "ResourceContents".getBytes(); //$NON-NLS-1$
            byte[] mfBytes =  metadataFactory.getBinaryVDBResource(null);
            byte[] rtmdBytes = runtimeMetadata.getBinaryVDBResource(null);
            assertEquals(expectedBytes.length, mfBytes.length);
            assertEquals(expectedBytes.length, rtmdBytes.length);
            for (int i = 0; i < expectedBytes.length; i++) {
                assertEquals("Byte at index " + i + " differs from expected content", expectedBytes[i], mfBytes[i]); //$NON-NLS-1$ //$NON-NLS-2$
                assertEquals("Byte at index " + i + " differs from expected content", expectedBytes[i], rtmdBytes[i]); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
     
    public void testGetCharacterVDBResource() {
        try {
            RuntimeMetadata runtimeMetadata = metadataFactory.createRuntimeMetadata();
            assertEquals("ResourceContents", metadataFactory.getCharacterVDBResource(null)); //$NON-NLS-1$
            assertEquals("ResourceContents", runtimeMetadata.getCharacterVDBResource(null)); //$NON-NLS-1$
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
     
}
