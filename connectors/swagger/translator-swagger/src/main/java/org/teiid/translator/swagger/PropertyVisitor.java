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

package org.teiid.translator.swagger;

import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.FileProperty;
import io.swagger.models.properties.MapProperty;
import io.swagger.models.properties.ObjectProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;

public abstract class PropertyVisitor {
    void visit(String name, RefProperty property) {
    }
    void visit(String name, ObjectProperty property) {
    }
    void visit(String name, MapProperty property) {
    }
    void visit(String name, FileProperty property) {
    }
    void visit(String name, ArrayProperty property) {
    }
    void visit(String name, Property property) {
    }

    void accept(String name, Property property) {
        if (property instanceof ArrayProperty) {
            visit(name, (ArrayProperty)property);
        } else if (property instanceof RefProperty) {
            visit(name, (RefProperty)property);
        } else if (property instanceof MapProperty) {
            visit(name, (MapProperty)property);
        } else if (property instanceof ObjectProperty) {
            visit(name, (ObjectProperty)property);
        } else if (property instanceof FileProperty) {
            visit(name, (FileProperty)property);
        } else {
            visit(name, property);
        }
    }
}
