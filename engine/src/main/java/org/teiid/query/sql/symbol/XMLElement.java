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

package org.teiid.query.sql.symbol;

import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Represents XMLElement
 */
public class XMLElement implements Expression {

    private static final long serialVersionUID = -3348922701950966494L;
    private String name;
    private XMLNamespaces namespaces;
    private XMLAttributes attributes;
    private List<Expression> content;

    public XMLElement(String name, List<Expression> content) {
        this.name = name;
        this.content = content;
    }

    public XMLAttributes getAttributes() {
        return attributes;
    }

    public XMLNamespaces getNamespaces() {
        return namespaces;
    }

    public void setAttributes(XMLAttributes attributes) {
        this.attributes = attributes;
    }

    public void setNamespaces(XMLNamespaces namespaces) {
        this.namespaces = namespaces;
    }

    public List<Expression> getContent() {
        return content;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setContent(List<Expression> args) {
        this.content = args;
    }

    @Override
    public Class<?> getType() {
        return DataTypeManager.DefaultDataClasses.XML;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public XMLElement clone() {
        XMLElement clone = new XMLElement(name, LanguageObject.Util.deepClone(content, Expression.class));
        if (namespaces != null) {
            clone.setNamespaces(namespaces.clone());
        }
        if (attributes != null) {
            clone.setAttributes(attributes.clone());
        }
        return clone;
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(name.toUpperCase().hashCode(), content.hashCode());
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof XMLElement)) {
            return false;
        }
        XMLElement other = (XMLElement)obj;
        return name.equalsIgnoreCase(other.name) && content.equals(other.content) && EquivalenceUtil.areEqual(this.namespaces, other.namespaces);
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
