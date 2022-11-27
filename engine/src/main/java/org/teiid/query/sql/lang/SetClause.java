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

package org.teiid.query.sql.lang;

import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.SQLStringVisitor;


public class SetClause implements LanguageObject {

    private static final long serialVersionUID = 8174681510498719451L;

    private ElementSymbol symbol;
    private Expression value;

    public SetClause(ElementSymbol symbol, Expression value) {
        ArgCheck.isNotNull(symbol);
        ArgCheck.isNotNull(value);
        this.symbol = symbol;
        this.value = value;
    }

    public ElementSymbol getSymbol() {
        return symbol;
    }

    public void setSymbol(ElementSymbol symbol) {
        this.symbol = symbol;
    }

    public Expression getValue() {
        return value;
    }

    public void setValue(Expression value) {
        this.value = value;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

    @Override
    public Object clone() {
        return new SetClause((ElementSymbol)symbol.clone(), (Expression)value.clone());
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(!(obj instanceof SetClause)) {
            return false;
        }

        SetClause other = (SetClause) obj;

        return this.symbol.equals(other.symbol) && this.value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(symbol.hashCode(), value.hashCode());
    }

}
