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

import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;

public class ExpressionCriteria extends PredicateCriteria {

    private Expression expression;

    public ExpressionCriteria(Expression expression) {
        this.expression = expression;
    }

    public Object clone() {
        return new ExpressionCriteria((Expression) expression.clone());
    }

    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public int hashCode() {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ExpressionCriteria)) {
            return false;
        }
        ExpressionCriteria other = (ExpressionCriteria) obj;
        return expression.equals(other.expression);
    }

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

}
