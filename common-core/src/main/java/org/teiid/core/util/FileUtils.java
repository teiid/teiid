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

package org.teiid.core.util;

import java.io.File;



public final class FileUtils {

    private FileUtils() {}

    public static String getBaseFileNameWithoutExtension(String path) {
        return StringUtil.getFirstToken(StringUtil.getLastToken(path, "/"), "."); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public static void removeDirectoryAndChildren(File directory) {
        removeChildrenRecursively(directory);
        if(!directory.delete()) {
            directory.deleteOnExit();
        }
    }

    public static void removeChildrenRecursively(File directory) {
        File[] files = directory.listFiles();
        if(files != null) {
            for(int i=0; i < files.length; i++) {
                File file = files[i];
                if (file.isDirectory()) {
                    removeDirectoryAndChildren(file);
                } else {
                    remove(file);
                }
            }
        }
    }

    public static void remove(File file) {
        if (file.exists()) {
            if(!file.delete()) {
                file.deleteOnExit();
            }
        }
    }

}