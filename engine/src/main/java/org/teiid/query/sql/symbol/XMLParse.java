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
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class XMLParse implements Expression {

    private boolean document;
    private Expression expression;
    private boolean wellFormed;

    @Override
    public Class<?> getType() {
        return DataTypeManager.DefaultDataClasses.XML;
    }

    public Expression getExpression() {
        return expression;
    }

    public boolean isDocument() {
        return document;
    }

    public void setDocument(boolean document) {
        this.document = document;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public boolean isWellFormed() {
        return wellFormed;
    }

    public void setWellFormed(boolean wellFormed) {
        this.wellFormed = wellFormed;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public XMLParse clone() {
        XMLParse clone = new XMLParse();
        clone.document = this.document;
        clone.expression = (Expression)this.expression.clone();
        clone.wellFormed = this.wellFormed;
        return clone;
    }

    @Override
    public int hashCode() {
        return expression.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof XMLParse)) {
            return false;
        }
        XMLParse other = (XMLParse)obj;
        return document == other.document
            && this.expression.equals(other.expression)
            && this.wellFormed == other.wellFormed;
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
