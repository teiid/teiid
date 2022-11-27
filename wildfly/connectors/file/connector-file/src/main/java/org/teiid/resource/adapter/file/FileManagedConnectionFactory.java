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

import java.util.Map;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.teiid.core.BundleUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class FileManagedConnectionFactory extends BasicManagedConnectionFactory{

    private static final long serialVersionUID = -1495488034205703625L;
    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(FileManagedConnectionFactory.class);

    private String parentDirectory;
    private String fileMapping;
    private boolean allowParentPaths = true;

    @Override
    @SuppressWarnings("serial")
    public BasicConnectionFactory<FileConnectionImpl> createConnectionFactory() throws ResourceException {
        if (this.parentDirectory == null) {
            throw new InvalidPropertyException(UTIL.getString("parentdirectory_not_set")); //$NON-NLS-1$
        }
        final Map<String, String> map = StringUtil.valueOf(this.fileMapping, Map.class);
        return new BasicConnectionFactory<FileConnectionImpl>() {

            @Override
            public FileConnectionImpl getConnection() throws ResourceException {
                return new FileConnectionImpl(parentDirectory, map, allowParentPaths);
            }
        };
    }

    public String getParentDirectory() {
        return parentDirectory;
    }

    public void setParentDirectory(String parentDirectory) {
        this.parentDirectory = parentDirectory;
    }

    public String getFileMapping() {
        return fileMapping;
    }

    public void setFileMapping(String fileMapping) {
        this.fileMapping = fileMapping;
    }

    public Boolean isAllowParentPaths() {
        return allowParentPaths;
    }

    public void setAllowParentPaths(Boolean allowParentPaths) {
        this.allowParentPaths = allowParentPaths != null && allowParentPaths;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (allowParentPaths ? 1231 : 1237);
        result = prime * result + ((fileMapping == null) ? 0 : fileMapping.hashCode());
        result = prime * result + ((parentDirectory == null) ? 0 : parentDirectory.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        FileManagedConnectionFactory other = (FileManagedConnectionFactory) obj;
        if (allowParentPaths != other.allowParentPaths)
            return false;
        if (!checkEquals(this.fileMapping, other.fileMapping)) {
            return false;
        }
        if (!checkEquals(this.parentDirectory, other.parentDirectory)) {
            return false;
        }
        return true;
    }

}
