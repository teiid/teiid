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

package org.teiid.adminapi.impl;

import org.teiid.adminapi.VDBImport;

public class VDBImportMetadata extends AdminObjectImpl implements VDBImport {

    private static final long serialVersionUID = 8827106139518843217L;

    private String name;
    private String version;
    private boolean importDataPolicies = true;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public boolean isImportDataPolicies() {
        return importDataPolicies;
    }

    public void setImportDataPolicies(boolean importDataPolicies) {
        this.importDataPolicies = importDataPolicies;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (importDataPolicies ? 1231 : 1237);
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        VDBImportMetadata other = (VDBImportMetadata) obj;
        if (importDataPolicies != other.importDataPolicies)
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "VDBImportMetadata [name=" + name + ", version=" + version //$NON-NLS-1$ //$NON-NLS-2$
                + ", importDataPolicies=" + importDataPolicies + "]"; //$NON-NLS-1$ //$NON-NLS-2$
    }

}
