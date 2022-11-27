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

package org.teiid.language;

import org.teiid.language.visitor.LanguageObjectVisitor;
import org.teiid.metadata.Column;

/**
 * Represents an element in the language.  An example of an element
 * would be a column reference in a SELECT clause.
 */
public class ColumnReference extends BaseLanguageObject implements MetadataReference<Column>, Expression {

    private NamedTable table;
    private String name;
    private Column metadataObject;
    private Class<?> type;

    public ColumnReference(NamedTable group, String name, Column metadataObject, Class<?> type) {
        this.table = group;
        this.name = name;
        this.metadataObject = metadataObject;
        this.type = type;
    }

    /**
     * Gets the name of the element.
     * @return the name of the element
     */
    public String getName() {
        return this.name;
    }

    /**
     * Return the table that contains this column.  May be null.
     * @return The group reference
     */
    public NamedTable getTable() {
        return table;
    }

    @Override
    public Column getMetadataObject() {
        return this.metadataObject;
    }

    public void setMetadataObject(Column metadataObject) {
        this.metadataObject = metadataObject;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }


    public void setTable(NamedTable group) {
        this.table = group;
    }

    public Class<?> getType() {
        return this.type;
    }

    /**
     * Sets the name of the element.
     * @param name The name of the element
     */
    public void setName(String name) {
        this.name = name;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

}
