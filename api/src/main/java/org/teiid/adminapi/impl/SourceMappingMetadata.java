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

import java.io.Serializable;

public class SourceMappingMetadata implements Serializable {
    private static final long serialVersionUID = -4417878417697685794L;

    private String name;
    private String jndiName;
    private String translatorName;

    public SourceMappingMetadata() {}

    public SourceMappingMetadata(String name, String translatorName, String connJndiName) {
        this.name = name;
        this.translatorName = translatorName;
        this.jndiName = connJndiName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     *
     * @return the jndi name or null if no connection factory is defined
     */
    public String getConnectionJndiName() {
        return jndiName;
    }

    public void setConnectionJndiName(String jndiName) {
        this.jndiName = jndiName;
    }

    public String getTranslatorName() {
        return translatorName;
    }

    public void setTranslatorName(String translatorName) {
        this.translatorName = translatorName;
    }

    public String toString() {
        return getName()+", "+getTranslatorName()+", "+getConnectionJndiName(); //$NON-NLS-1$ //$NON-NLS-2$
    }
}