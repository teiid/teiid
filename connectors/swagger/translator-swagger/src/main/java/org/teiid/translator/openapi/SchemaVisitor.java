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

package org.teiid.translator.openapi;

import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.FileSchema;
import io.swagger.v3.oas.models.media.MapSchema;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;

public abstract class SchemaVisitor {
    /**
     *
     * @param name
     * @param property
     */
    void visit(String name, ObjectSchema property) {
    }
    /**
     *
     * @param name
     * @param property
     */
    void visit(String name, MapSchema property) {
    }
    /**
     *
     * @param name
     * @param property
     */
    void visit(String name, FileSchema property) {
    }
    /**
     *
     * @param name
     * @param property
     */
    void visit(String name, ArraySchema property) {
    }
    /**
     *
     * @param name
     * @param property
     */
    void visit(String name, Schema<?> property) {
    }

    void accept(String name, Schema<?> property) {
        if (property instanceof ArraySchema) {
            visit(name, (ArraySchema)property);
        } else if (property instanceof MapSchema) {
            visit(name, (MapSchema)property);
        } else if (property instanceof ObjectSchema) {
            visit(name, (ObjectSchema)property);
        } else if (property instanceof FileSchema) {
            visit(name, (FileSchema)property);
        } else {
            visit(name, property);
        }
    }
}
