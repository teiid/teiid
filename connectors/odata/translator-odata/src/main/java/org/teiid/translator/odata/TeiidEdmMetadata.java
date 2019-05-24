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
package org.teiid.translator.odata;

import org.odata4j.edm.EdmComplexType;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmFunctionImport;
import org.odata4j.edm.EdmSchema;
import org.odata4j.edm.EdmType;
import org.odata4j.internal.EdmDataServicesDecorator;

public class TeiidEdmMetadata extends EdmDataServicesDecorator {
    private EdmDataServices delegate;
    private String schemaName;

    public TeiidEdmMetadata(String schemaName, EdmDataServices delegate) {
        this.schemaName = schemaName;
        this.delegate = delegate;
    }

    @Override
    protected EdmDataServices getDelegate() {
        return this.delegate;
    }

    @Override
    public EdmType findEdmEntityType(String name) {
        return super.findEdmEntityType(teiidSchemaBasedName(name));
    }

    private String teiidSchemaBasedName(String name) {
        return this.schemaName+"."+name; //$NON-NLS-1$
    }

    @Override
    public EdmEntitySet findEdmEntitySet(String name) {
        return super.findEdmEntitySet(teiidSchemaBasedName(name));
    }

    @Override
    public EdmComplexType findEdmComplexType(String complexTypeFQName) {
        return super.findEdmComplexType(teiidSchemaBasedName(complexTypeFQName));
    }

    @Override
    public EdmFunctionImport findEdmFunctionImport(String functionImportName) {
        return super.findEdmFunctionImport(teiidSchemaBasedName(functionImportName));
    }

    @Override
    public EdmSchema findSchema(String namespace) {
        return super.findSchema(this.schemaName);
    }
}