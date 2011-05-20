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
package org.teiid.dqp.internal.datamgr;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jboss.virtual.VirtualFile;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.TransformationMetadata.Resource;

@SuppressWarnings("nls")
public class TestMetadataFactory {
    private static final String MY_RESOURCE_PATH = "my/resource/path";
	private RuntimeMetadataImpl metadataFactory;
    
    @Before public void setUp() throws IOException{
        MetadataStore metadataStore = new MetadataStore();
        CompositeMetadataStore store = new CompositeMetadataStore(metadataStore);
    	VDBMetaData vdbMetaData = new VDBMetaData();
    	vdbMetaData.setName("foo"); //$NON-NLS-1$
    	vdbMetaData.setVersion(1);
    	Map<String, Resource> vdbEntries = new LinkedHashMap<String, Resource>();
    	VirtualFile vf = Mockito.mock(VirtualFile.class);
    	Mockito.stub(vf.openStream()).toReturn(new ByteArrayInputStream("ResourceContents".getBytes()));
    	vdbEntries.put(MY_RESOURCE_PATH, new Resource(vf, true));
        metadataFactory = new RuntimeMetadataImpl(new TransformationMetadata(vdbMetaData, store, vdbEntries, null, null));
    }
    
    @Test public void testGetVDBResourcePaths() throws Exception {
        String[] expectedPaths = new String[] {MY_RESOURCE_PATH}; //$NON-NLS-1$
        String[] mfPaths = metadataFactory.getVDBResourcePaths();
        assertEquals(expectedPaths.length, mfPaths.length);
        for (int i = 0; i < expectedPaths.length; i++) {
            assertEquals(expectedPaths[i], mfPaths[i]);
        }
    }
     
    @Test public void testGetBinaryVDBResource() throws Exception {
        byte[] expectedBytes = "ResourceContents".getBytes(); //$NON-NLS-1$
        byte[] mfBytes =  metadataFactory.getBinaryVDBResource(MY_RESOURCE_PATH);
        assertEquals(expectedBytes.length, mfBytes.length);
        for (int i = 0; i < expectedBytes.length; i++) {
            assertEquals("Byte at index " + i + " differs from expected content", expectedBytes[i], mfBytes[i]); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }
     
    @Test public void testGetCharacterVDBResource() throws Exception {
        assertEquals("ResourceContents", metadataFactory.getCharacterVDBResource(MY_RESOURCE_PATH)); //$NON-NLS-1$
    }
     
}
