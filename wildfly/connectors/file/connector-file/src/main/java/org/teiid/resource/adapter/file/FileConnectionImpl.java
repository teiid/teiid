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
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

import org.teiid.file.JavaVirtualFileConnection;
import org.teiid.resource.spi.ResourceConnection;
import org.teiid.translator.TranslatorException;


public class FileConnectionImpl extends JavaVirtualFileConnection implements ResourceConnection {

    private Map<String, String> fileMapping;
    private boolean allowParentPaths;
    private static final Pattern parentRef = Pattern.compile("(^\\.\\.(\\\\{2}|/)?.*)|((\\\\{2}|/)\\.\\.)"); //$NON-NLS-1$

    public FileConnectionImpl(String parentDirectory, Map<String, String> fileMapping, boolean allowParentPaths) {
        super(parentDirectory);
        if (fileMapping == null) {
            fileMapping = Collections.emptyMap();
        }
        this.fileMapping = fileMapping;
        this.allowParentPaths = allowParentPaths;
    }

    @Override
    protected java.io.File relativeFile(String path) throws TranslatorException {
        String altPath = fileMapping.get(path);
        if (altPath != null) {
            path = altPath;
        }
        if (!allowParentPaths && parentRef.matcher(path).matches()) {
            throw new TranslatorException(FileManagedConnectionFactory.UTIL.getString("parentpath_not_allowed", path)); //$NON-NLS-1$
        }
        return new File(parentDirectory, path);
    }

}
