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

import org.teiid.adminapi.Translator;


public class VDBTranslatorMetaData extends AdminObjectImpl implements Translator {
    private static final long serialVersionUID = -3454161477587996138L;
    private String type;
    private Class<?> executionClass;
    private String description;
    private String moduleName;
    private transient VDBTranslatorMetaData parent;

    @Override
    public String getName() {
        return super.getName();
    }

    public void setName(String name) {
        super.setName(name);
    }

    @Override
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String toString() {
        return getName();
    }

    public Class<?> getExecutionFactoryClass() {
        if (this.executionClass == null && this.parent != null) {
            return this.parent.getExecutionFactoryClass();
        }
        return this.executionClass;
    }

    public void setExecutionFactoryClass(Class<?> clazz) {
        this.executionClass = clazz;
        addProperty(EXECUTION_FACTORY_CLASS, clazz.getName());
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String desc) {
        this.description = desc;
    }

    public String getModuleName() {
        return this.moduleName;
    }

    public void setModuleName(String name) {
        this.moduleName = name;
    }

    public void setParent(VDBTranslatorMetaData parent) {
        this.parent = parent;
    }

    public VDBTranslatorMetaData getParent() {
        return parent;
    }
}
