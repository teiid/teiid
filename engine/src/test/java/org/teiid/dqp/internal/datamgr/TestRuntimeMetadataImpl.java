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

import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.NioVirtualFile;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.metadata.VDBResources;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestRuntimeMetadataImpl {
    private static final String MY_RESOURCE_PATH = "my/resource/path";
    private RuntimeMetadataImpl runtimeMetadata;

    @BeforeClass public static void beforeClass() throws IOException {
        FileWriter f = new FileWriter(UnitTestUtil.getTestScratchPath()+"/foo");
        f.write("ResourceContents");
        f.close();
    }

    @Before public void setUp() {
        VDBMetaData vdbMetaData = RealMetadataFactory.example1VDB();
        vdbMetaData.getModel("pm1").setVisible(false);
        Map<String, VDBResources.Resource> vdbEntries = new LinkedHashMap<String, VDBResources.Resource>();
        vdbEntries.put(MY_RESOURCE_PATH,
                new VDBResources.Resource(new NioVirtualFile(
                        UnitTestUtil.getTestScratchFile("foo").toPath())));
        TransformationMetadata metadata = new TransformationMetadata(vdbMetaData, new CompositeMetadataStore(RealMetadataFactory.example1Store()), vdbEntries, null, null);
        metadata.setHiddenResolvable(false);
        runtimeMetadata = new RuntimeMetadataImpl(metadata);
    }

    @Test public void testGetVDBResourcePaths() throws Exception {
        String[] expectedPaths = new String[] {MY_RESOURCE_PATH}; //$NON-NLS-1$
        String[] mfPaths = runtimeMetadata.getVDBResourcePaths();
        assertEquals(expectedPaths.length, mfPaths.length);
        for (int i = 0; i < expectedPaths.length; i++) {
            assertEquals(expectedPaths[i], mfPaths[i]);
        }
    }

    @Test public void testGetBinaryVDBResource() throws Exception {
        byte[] expectedBytes = "ResourceContents".getBytes(); //$NON-NLS-1$
        byte[] mfBytes =  runtimeMetadata.getBinaryVDBResource(MY_RESOURCE_PATH);
        assertEquals(expectedBytes.length, mfBytes.length);
        for (int i = 0; i < expectedBytes.length; i++) {
            assertEquals("Byte at index " + i + " differs from expected content", expectedBytes[i], mfBytes[i]); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    @Test public void testGetCharacterVDBResource() throws Exception {
        assertEquals("ResourceContents", runtimeMetadata.getCharacterVDBResource(MY_RESOURCE_PATH)); //$NON-NLS-1$
    }

    @Test public void testHidden() throws Exception {
        assertNotNull(runtimeMetadata.getTable("pm1.g1"));
    }

}
