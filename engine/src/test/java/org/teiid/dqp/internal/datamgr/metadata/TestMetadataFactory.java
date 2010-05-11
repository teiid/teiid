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

/*
 */
package org.teiid.dqp.internal.datamgr.metadata;

import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.query.unittest.FakeMetadataFacade;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.unittest.FakeMetadataObject;
import org.teiid.query.unittest.FakeMetadataStore;

import junit.framework.TestCase;


public class TestMetadataFactory  extends TestCase {
    private RuntimeMetadataImpl metadataFactory;
    private FakeMetadataObject pm1g1;
    
    public TestMetadataFactory(String name) {
        super(name);
    }
    
    public void setUp(){
        FakeMetadataStore store = new FakeMetadataStore();
        pm1g1 = FakeMetadataFactory.createPhysicalGroup("pm1.g1", FakeMetadataFactory.createPhysicalModel("pm1.g1")); //$NON-NLS-1$ //$NON-NLS-2$
        List pm1g1e = FakeMetadataFactory.createElements(pm1g1, 
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        store.addObject(pm1g1);
        store.addObjects(pm1g1e);
        metadataFactory = new RuntimeMetadataImpl(new FakeMetadataFacade(store));
    }
    
    public void testGetVDBResourcePaths() throws Exception {
        String[] expectedPaths = new String[] {"my/resource/path"}; //$NON-NLS-1$
        String[] mfPaths = metadataFactory.getVDBResourcePaths();
        assertEquals(expectedPaths.length, mfPaths.length);
        for (int i = 0; i < expectedPaths.length; i++) {
            assertEquals(expectedPaths[i], mfPaths[i]);
        }
    }
     
    public void testGetBinaryVDBResource() throws Exception {
        byte[] expectedBytes = "ResourceContents".getBytes(); //$NON-NLS-1$
        byte[] mfBytes =  metadataFactory.getBinaryVDBResource(null);
        assertEquals(expectedBytes.length, mfBytes.length);
        for (int i = 0; i < expectedBytes.length; i++) {
            assertEquals("Byte at index " + i + " differs from expected content", expectedBytes[i], mfBytes[i]); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }
     
    public void testGetCharacterVDBResource() throws Exception {
        assertEquals("ResourceContents", metadataFactory.getCharacterVDBResource(null)); //$NON-NLS-1$
    }
     
}
