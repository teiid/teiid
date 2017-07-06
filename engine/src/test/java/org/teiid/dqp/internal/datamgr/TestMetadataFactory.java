/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 */
package org.teiid.dqp.internal.datamgr;

import static org.junit.Assert.*;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.MetadataStore;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.VDBResources;

@SuppressWarnings("nls")
public class TestMetadataFactory {
    private static final String MY_RESOURCE_PATH = "my/resource/path";
	private RuntimeMetadataImpl metadataFactory;
	static VirtualFile root;
	static Closeable fileMount;
	
	@BeforeClass public static void beforeClass() throws IOException {
    	FileWriter f = new FileWriter(UnitTestUtil.getTestScratchPath()+"/foo");
    	f.write("ResourceContents");
    	f.close();
    	
    	root = VFS.getChild("location");
    	fileMount = VFS.mountReal(new File(UnitTestUtil.getTestScratchPath()), root);		
	}
	
	@AfterClass public static void afterClass() throws IOException {
		fileMount.close();
	}
    
    @Before public void setUp() {
        MetadataStore metadataStore = new MetadataStore();
        CompositeMetadataStore store = new CompositeMetadataStore(metadataStore);
    	VDBMetaData vdbMetaData = new VDBMetaData();
    	vdbMetaData.setName("foo"); //$NON-NLS-1$
    	vdbMetaData.setVersion(1);
    	Map<String, VDBResources.Resource> vdbEntries = new LinkedHashMap<String, VDBResources.Resource>();
    	vdbEntries.put(MY_RESOURCE_PATH, new VDBResources.Resource(root.getChild("foo")));
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
