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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.resource.ResourceException;

import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.file.JavaVirtualFile;
import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.resource.spi.ResourceConnection;
import org.teiid.translator.TranslatorException;


public class FileConnectionImpl implements VirtualFileConnection, ResourceConnection {

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

        Path parentPath = parentDirectory.toPath();

        if (datafile.isDirectory()) {
            return convert(datafile.listFiles(), parentPath);
        }

        if (datafile.exists()) {
            return new VirtualFile[] {pathToVirtualFile(parentPath, datafile.toPath())};
        }

        if (location.contains("*")) { //$NON-NLS-1$

            //for backwards compatibility support any wildcard, but no escapes or other glob searches
            location = location.replaceAll("\\\\", "\\\\\\\\"); //$NON-NLS-1$ //$NON-NLS-2$
            location = location.replaceAll("\\?", "\\\\?"); //$NON-NLS-1$ //$NON-NLS-2$
            location = location.replaceAll("\\[", "\\\\["); //$NON-NLS-1$ //$NON-NLS-2$
            location = location.replaceAll("\\{", "\\\\{"); //$NON-NLS-1$ //$NON-NLS-2$

            List<String> parts = StringUtil.split(location, "/"); //$NON-NLS-1$ //split on the file separator

            try {
                List<Iterable<Path>> toProcess = new LinkedList<>();
                toProcess.add(Files.newDirectoryStream(parentPath, parts.get(0)));
                for (int i = 1; i < parts.size(); i++) {
                    int size = toProcess.size();
                    if (size == 0) {
                        break;
                    }
                    for (int j = 0;  j < size; j++) {
                        Iterable<Path> paths = toProcess.remove(0);
                        for (Path p : paths) {
                            if (Files.isRegularFile(p)) {
                                continue;
                            }
                            toProcess.add(Files.newDirectoryStream(p, parts.get(i)));
                        }
                    }
                }
                List<VirtualFile> result = new ArrayList<>();
                for (Iterable<Path> paths : toProcess) {
                    for (Path p : paths) {
                        JavaVirtualFile f = pathToVirtualFile(parentPath, p);
                        result.add(f);
                    }
                }
                return result.toArray(new VirtualFile[result.size()]);
            } catch (IOException e) {
                throw new TranslatorException(e);
            }
        }
        return null;
    }

    private JavaVirtualFile pathToVirtualFile(Path parentPath, Path p) {
        Path subpath = p;
        if (p.startsWith(parentPath)) {
            subpath = p.subpath(parentPath.getNameCount(), p.getNameCount());
        }
        JavaVirtualFile f = new JavaVirtualFile(p.toFile(), subpath.toString());
        return f;
    }

    VirtualFile[] convert(File[] files, Path parentPath) {
        VirtualFile[] result = new VirtualFile[files.length];
        for (int i = 0; i < files.length; i++) {
            result[i] = pathToVirtualFile(parentPath, files[i].toPath());
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
