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

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class DerivedColumn implements LanguageObject {

    private String alias;
    private Expression expression;
    private boolean propagateName = true;

    public DerivedColumn(String name, Expression expression) {
        this.alias = name;
        this.expression = expression;
    }

    public boolean isPropagateName() {
        return propagateName;
    }

    public void setPropagateName(boolean propagateName) {
        this.propagateName = propagateName;
    }

    public String getAlias() {
        return alias;
    }

    public Expression getExpression() {
        return expression;
    }

    public void setAlias(String name) {
        this.alias = name;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(0, alias, expression);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof DerivedColumn)) {
            return false;
        }
        DerivedColumn other = (DerivedColumn)obj;
        return EquivalenceUtil.areEqual(alias, other.alias) && this.expression.equals(other.expression);
    }

    @Override
    public DerivedColumn clone() {
        DerivedColumn clone = new DerivedColumn(alias, (Expression)this.expression.clone());
        clone.propagateName = propagateName;
        return clone;
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
