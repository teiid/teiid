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

package org.teiid.file;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Test;
import org.teiid.core.types.InputStreamFactory.StorageMode;

public class TestVirtualFile {

    @Test public void testGetStorageMode() {
        VirtualFile vf = new VirtualFile() {

            @Override
            public OutputStream openOutputStream(boolean lock) throws IOException {
                return null;
            }

            @Override
            public InputStream openInputStream(boolean lock) throws IOException {
                return null;
            }

            @Override
            public long getSize() {
                return 0;
            }

            @Override
            public String getName() {
                return null;
            }

            @Override
            public long getLastModified() {
                return 0;
            }

            @Override
            public long getCreationTime() {
                return 0;
            }

            @Override
            public String getPath() {
                return null;
            }

            @Override
            public boolean isDirectory() {
                return false;
            }
        };

        assertEquals(StorageMode.OTHER, vf.createInputStreamFactory().getStorageMode());
    }

}
