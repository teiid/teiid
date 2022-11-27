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
package org.teiid.metadata;

/**
 * Represents a Server and its properties. Distinction is this is NOT connection,
 * you can create connections to Server.
 */
public class Server extends AbstractMetadataRecord {
    private static final long serialVersionUID = -3969389574210542638L;
    private String type;
    private String version;
    private String dataWrapperName;

    public Server(String name) {
        super.setName(name);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDataWrapper() {
        return dataWrapperName;
    }

    public void setDataWrapper(String wrapperName) {
        this.dataWrapperName = wrapperName;
    }

    public String getResourceName() {
        String result = getProperty("resource-name", false);//$NON-NLS-1$
        if (result != null) {
            return result;
        }
        return getProperty("jndi-name", false); //$NON-NLS-1$
    }

    public void setResourceName(String value) {
        setProperty("resource-name", value); //$NON-NLS-1$
    }

}
