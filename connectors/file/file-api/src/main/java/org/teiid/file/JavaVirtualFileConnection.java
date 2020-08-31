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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.translator.TranslatorException;


public class JavaVirtualFileConnection implements VirtualFileConnection {

    protected File parentDirectory;

    public JavaVirtualFileConnection(String parentDirectory) {
        this.parentDirectory = new File(parentDirectory);
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

        List<String> globParts = new ArrayList<>();
        while (!datafile.exists() && !datafile.equals(parentPath)) {
            String name = datafile.getName();
            String globPath = parseGlob(name);
            globParts.add(0, globPath);
            datafile = datafile.getParentFile();
        }

        if (globParts.isEmpty()) {
            if (datafile.isDirectory()) {
                return convert(datafile.listFiles(), parentPath);
            }

            if (datafile.exists()) {
                return new VirtualFile[] {pathToVirtualFile(parentPath, datafile.toPath())};
            }
        }

        try {
            List<Iterable<Path>> toProcess = new LinkedList<>();
            toProcess.add(Files.newDirectoryStream(datafile.toPath(), globParts.get(0)));
            for (int i = 1; i < globParts.size(); i++) {
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
                        toProcess.add(Files.newDirectoryStream(p, globParts.get(i)));
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

    public File getFile(String path) throws TranslatorException {
        if (path == null) {
            return this.parentDirectory;
        }
        return relativeFile(path);
    }

    protected File relativeFile(String path) throws TranslatorException {
        return new File(parentDirectory, path);
    }

    @Override
    public void close() {

    }

    private String parseGlob(String location) {
        StringBuilder part = new StringBuilder();
        for (int i = 0; i < location.length(); i++) {
            char c = location.charAt(i);
            switch (c) {
            //escape the other glob metacharacters
            case '?':
            case '[':
            case '{':
            case '\\':
                part.append('\\');
                break;
            case '*':
                if (i < location.length() -1 && location.charAt(i+1) == '*') {
                    part.append('\\');
                    i++;
                }
                break;
            }
            part.append(c);
        }
        return part.toString();
    }

}
