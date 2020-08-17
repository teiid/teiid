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

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.teiid.connector.DataPlugin;
import org.teiid.resource.api.Connection;
import org.teiid.translator.TranslatorException;

/**
 * Simple interface for the filesystem
 */
public interface VirtualFileConnection extends Connection {

    public static class FileMetadata {
        private Long size;

        public Long size() {
            return size;
        }

        public FileMetadata size(Long s) {
            this.size = s;
            return this;
        }
    }

    /**
     * Return an array of files by a given file pattern
     *
     * @param namePattern
     *            - the syntax and pattern. The wildcard character * will
     *            initially be treated as a literal match (there is currently no
     *            escaping supported), then as a non-recursive glob search. <br>
     *            For example the search /* would return all of the top level
     *            directory contents - but not expand that to any
     *            subdirectories. The wildcard may be used in both directories
     *            and filenames.
     * @return the virtual files found or null if a non-glob file match could
     *         not be found
     * @throws TranslatorException
     */
    VirtualFile[] getFiles(String namePattern) throws TranslatorException;

    /**
     * Add a file
     * @param fileMetadata Additional metadata about the file to be created.  May not be used/supported by all file sources.
     * @throws TranslatorException
     */
    default void add(InputStream in, String path, FileMetadata fileMetadata) throws TranslatorException {
        add(in, path);
    }

    /**
     * Add a file
     * @throws TranslatorException
     */
    void add(InputStream in, String path) throws TranslatorException;

    /**
     * Remove a file
     * @param path
     * @return
     * @throws TranslatorException
     */
    boolean remove(String path) throws TranslatorException;

    default boolean areFilesUsableAfterClose() {
        return true;
    }

    public static class Util {

        /**
         * Gets the file or files, if the path is a directory, at the given path.
         * Note the path can only refer to a single directory - directories are not recursively scanned.
         * @param exceptionIfFileNotFound
         * @return the files or may be null if not found
         */
        public static VirtualFile[] getFiles(String location, VirtualFileConnection fc, boolean exceptionIfFileNotFound, boolean includeDirectories) throws TranslatorException {
            VirtualFile[] files = fc.getFiles(location);
            if (!includeDirectories && files != null) {
                Collection<VirtualFile> filtered = Arrays.asList(files).stream().filter((f)->!f.isDirectory()).collect(Collectors.toList());
                if (filtered.size() != files.length) {
                    files = filtered.toArray(new VirtualFile[filtered.size()]);
                }
            }
            if (exceptionIfFileNotFound && (files == null || files.length == 0) ) {
                throw new TranslatorException(DataPlugin.Util.gs("file_not_found", location)); //$NON-NLS-1$
            }

            return files;
        }

    }

}
