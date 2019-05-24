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

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class XMLSerialize implements Expression {

    private Boolean document;
    private Boolean declaration;
    private Expression expression;
    private String typeString;
    private Class<?> type;
    private String version;
    private String encoding;

    @Override
    public Class<?> getType() {
        if (type == null) {
            if (typeString == null) {
                type = DataTypeManager.DefaultDataClasses.CLOB;
            } else {
                type = DataTypeManager.getDataTypeClass(typeString);
            }
        }
        return type;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Boolean getDeclaration() {
        return declaration;
    }

    public void setDeclaration(Boolean declaration) {
        this.declaration = declaration;
    }

    public Expression getExpression() {
        return expression;
    }

    public Boolean getDocument() {
        return document;
    }

    public void setDocument(Boolean document) {
        this.document = document;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public void setTypeString(String typeString) {
        this.typeString = typeString;
    }

    public String getTypeString() {
        return typeString;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public XMLSerialize clone() {
        XMLSerialize clone = new XMLSerialize();
        clone.document = this.document;
        clone.expression = (Expression)this.expression.clone();
        clone.typeString = this.typeString;
        clone.type = this.type;
        clone.declaration = this.declaration;
        clone.version = this.version;
        clone.encoding = this.encoding;
        return clone;
    }

    public boolean isDocument() {
        return document != null && document;
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(expression.hashCode(), getType());
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof XMLSerialize)) {
            return false;
        }
        XMLSerialize other = (XMLSerialize)obj;
        return EquivalenceUtil.areEqual(this.document, other.document)
            && this.expression.equals(other.expression)
            && this.getType() == other.getType()
            && EquivalenceUtil.areEqual(this.declaration, other.declaration)
            && EquivalenceUtil.areEqual(this.version, other.version)
            && EquivalenceUtil.areEqual(this.encoding, other.encoding);
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
