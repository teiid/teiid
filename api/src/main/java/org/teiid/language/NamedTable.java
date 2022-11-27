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

import java.util.Objects;

import org.teiid.language.visitor.LanguageObjectVisitor;
import org.teiid.metadata.Table;

/**
 * Represents a table in the language objects.  An example would
 * be a table reference in the FROM clause.
 */
public class NamedTable extends BaseLanguageObject implements MetadataReference<Table>, TableReference {

    private String correlationName;
    private String name;
    private Table metadataObject;

    public NamedTable(String name, String correlationName, Table group) {
        this.name = name;
        this.correlationName = correlationName;
        this.metadataObject = group;
    }

    public String getCorrelationName() {
        return correlationName;
    }

    /**
     * Gets the name of the table.  Will typically match the name in the metadata.
     * @return
     */
    public String getName() {
        return this.name;
    }

    @Override
    public Table getMetadataObject() {
        return this.metadataObject;
    }

    public void setMetadataObject(Table metadataObject) {
        this.metadataObject = metadataObject;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setName(String definition) {
        this.name = definition;
    }

    public void setCorrelationName(String context) {
        this.correlationName = context;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, correlationName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof NamedTable)) {
            return false;
        }
        NamedTable other = (NamedTable) obj;
        if (!Objects.equals(this.metadataObject, other.metadataObject)) {
            return false;
        }
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if ((this.correlationName != null && !this.correlationName.equalsIgnoreCase(other.correlationName))
        || (this.correlationName == null && other.correlationName != null)) {
            return false;
        }
        return true;
    }

}
