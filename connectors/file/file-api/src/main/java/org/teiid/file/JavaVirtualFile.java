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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.Map;

import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.InputStreamFactory.FileInputStreamFactory;
import org.teiid.core.types.InputStreamFactory.StorageMode;

public class JavaVirtualFile implements VirtualFile {

    private final File f;
    private final String path;

    public JavaVirtualFile(File f) {
        this.f = f;
        this.path = f.getPath();
    }

    public JavaVirtualFile(File f, String path) {
        this.f = f;
        this.path = path;
    }

    @Override
    public String getName() {
        return f.getName();
    }

    @Override
    public boolean isDirectory() {
        return f.isDirectory();
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public long getLastModified() {
        return f.lastModified();
    }

    @Override
    public long getCreationTime() {
        try {
            Map<String, Object> attributes = Files.readAttributes(f.toPath(), "creationTime"); //$NON-NLS-1$
            return ((FileTime)attributes.get("creationTime")).toMillis(); //$NON-NLS-1$
        } catch (IOException e) {
        }
        return f.lastModified();
    }

    @Override
    public long getSize() {
        return f.length();
    }

    @Override
    public InputStream openInputStream(boolean lock) throws IOException {
        FileInputStream fis = new FileInputStream(f);
        if (lock) {
            try {
                fis.getChannel().tryLock(0, Long.MAX_VALUE, true);
            } catch (OverlappingFileLockException e) {
                fis.getChannel().lock(); //try a blocking exclusive lock instead
            }
        }
        return fis;
    }

    @Override
    public OutputStream openOutputStream(boolean lock) throws IOException {
        FileOutputStream fos = new FileOutputStream(f);
        if (lock) {
            fos.getChannel().lock();
        }
        return fos;
    }

    @Override
    public InputStreamFactory createInputStreamFactory() {
        return new FileInputStreamFactory(f);
    }

    @Override
    public StorageMode getStorageMode() {
        return StorageMode.PERSISTENT;
    }
}