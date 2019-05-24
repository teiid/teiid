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

package org.teiid.resource.adapter.file;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

import javax.resource.ResourceException;

import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.file.JavaVirtualFile;
import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;


public class FileConnectionImpl extends BasicConnection implements VirtualFileConnection {

    private File parentDirectory;
    private Map<String, String> fileMapping;
    private boolean allowParentPaths;
    private static final Pattern parentRef = Pattern.compile("(^\\.\\.(\\\\{2}|/)?.*)|((\\\\{2}|/)\\.\\.)"); //$NON-NLS-1$

    public FileConnectionImpl(String parentDirectory, Map<String, String> fileMapping, boolean allowParentPaths) {
        this.parentDirectory = new File(parentDirectory);
        if (fileMapping == null) {
            fileMapping = Collections.emptyMap();
        }
        this.fileMapping = fileMapping;
        this.allowParentPaths = allowParentPaths;
    }

    @Override
    public void add(InputStream in, String path)
            throws TranslatorException {
        try {
            ObjectConverterUtil.write(in, getFile(path));
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public boolean remove(String path) throws TranslatorException {
        File f = getFile(path);
        if (!f.exists()) {
            return false;
        }
        return f.delete();
    }

    @Override
    public VirtualFile[] getFiles(String location)
            throws TranslatorException {
        File datafile = getFile(location);

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

    VirtualFile[] convert(File[] files) {
        VirtualFile[] result = new VirtualFile[files.length];
        for (int i = 0; i < files.length; i++) {
            result[i] = new JavaVirtualFile(files[i]);
        }
        return result;
    }

    File getFile(String path) throws TranslatorException {
        if (path == null) {
            return this.parentDirectory;
        }
        String altPath = fileMapping.get(path);
        if (altPath != null) {
            path = altPath;
        }
        if (!allowParentPaths && parentRef.matcher(path).matches()) {
            throw new TranslatorException(FileManagedConnectionFactory.UTIL.getString("parentpath_not_allowed", path)); //$NON-NLS-1$
        }
        return new File(parentDirectory, path);
    }

    @Override
    public void close() throws ResourceException {

    }

}
