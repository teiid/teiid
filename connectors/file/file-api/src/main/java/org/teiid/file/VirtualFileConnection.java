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

import org.teiid.connector.DataPlugin;
import org.teiid.resource.api.Connection;
import org.teiid.translator.TranslatorException;

/**
 * Simple interface for the filesystem
 */
public interface VirtualFileConnection extends Connection {

    /**
     * Return a list of files by a given file pattern
     * @param namePattern - the syntax and pattern
     * @return
     * @throws TranslatorException
     */
    VirtualFile[] getFiles(String namePattern) throws TranslatorException;

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

    public static class Util {

        /**
         * Gets the file or files, if the path is a directory, at the given path.
         * Note the path can only refer to a single directory - directories are not recursively scanned.
         * @param exceptionIfFileNotFound
         * @return
         */
        public static VirtualFile[] getFiles(String location, VirtualFileConnection fc, boolean exceptionIfFileNotFound) throws TranslatorException {
            VirtualFile[] files = fc.getFiles(location);
            if (files == null && exceptionIfFileNotFound) {
                throw new TranslatorException(DataPlugin.Util.gs("file_not_found", location)); //$NON-NLS-1$
            }

            return files;
        }

    }

}
