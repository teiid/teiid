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
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.FileTime;
import java.util.Map;

import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.InputStreamFactory.FileInputStreamFactory;

public class JavaVirtualFile implements VirtualFile {

    private final File f;

    public JavaVirtualFile(File f) {
        this.f = f;
    }

    @Override
    public String getName() {
        return f.getName();
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

    public static VirtualFile[] getFiles(String location, File datafile) {
        if (datafile.isDirectory()) {
            return convert(datafile.listFiles());
        }

        if (datafile.exists()) {
            return new VirtualFile[] {new JavaVirtualFile(datafile)};
        }

        File parentDir = datafile.getParentFile();

        if (parentDir == null || !parentDir.exists()) {
            return null;
        }

        if (location.contains("*")) { //$NON-NLS-1$
            //for backwards compatibility support any wildcard, but no escapes or other glob searches
            location = location.replaceAll("\\\\", "\\\\\\\\"); //$NON-NLS-1$ //$NON-NLS-2$
            location = location.replaceAll("\\?", "\\\\?"); //$NON-NLS-1$ //$NON-NLS-2$
            location = location.replaceAll("\\[", "\\\\["); //$NON-NLS-1$ //$NON-NLS-2$
            location = location.replaceAll("\\{", "\\\\{"); //$NON-NLS-1$ //$NON-NLS-2$

            final PathMatcher matcher =
                    FileSystems.getDefault().getPathMatcher("glob:" + location); //$NON-NLS-1$

            FileFilter fileFilter = new FileFilter() {

                @Override
                public boolean accept(File pathname) {
                    return pathname.isFile() && matcher.matches(FileSystems.getDefault().getPath(pathname.getName()));
                }
            };

            return convert(parentDir.listFiles(fileFilter));
        }

        return null;
    }

    public static VirtualFile[] convert(File[] files) {
        VirtualFile[] result = new VirtualFile[files.length];
        for (int i = 0; i < files.length; i++) {
            result[i] = new JavaVirtualFile(files[i]);
        }
        return result;
    }
}