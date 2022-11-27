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
package org.teiid.olingo;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.edm.EdmStructuredType;

public class ComplexReturnType {
    private Entity entity;
    private String name;
    private boolean expand;
    private EdmStructuredType type;

    public ComplexReturnType(String name, EdmStructuredType type, Entity entity, boolean expand) {
        this.name = name;
        this.type = type;
        this.entity = entity;
        this.expand = expand;
    }

    public Entity getEntity() {
        return entity;
    }

    public String getName() {
        return name;
    }

    public boolean isExpand() {
        return expand;
    }

    public EdmStructuredType getEdmStructuredType() {
        return type;
    }
}