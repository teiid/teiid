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

import org.teiid.query.sql.LanguageVisitor;

public class ExpressionSymbol extends Symbol implements DerivedExpression {
    private Expression expression;

    /**
     * Construct an ExpressionSymbol with name and expression.
     */
    public ExpressionSymbol(String name, Expression expression) {
        super(name);
        this.expression = expression;
    }

    /**
     * Get the expression for this symbol
     * @return Expression for this symbol
     */
    public Expression getExpression() {
        return this.expression;
    }

    /**
       * Set the expression represented by this symbol.
       * @param expression Expression for this expression symbol
      */
    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    /**
     * Get the type of the symbol
     * @return Type of the symbol, may be null before resolution
     */
    public Class getType() {
        return this.expression.getType();
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Return a deep copy of this object
     * @return Deep copy of this object
     */
    public Object clone() {
        Expression clonedExpr = null;
        if(getExpression() != null) {
            clonedExpr = (Expression) getExpression().clone();
        }
        ExpressionSymbol copy = new ExpressionSymbol(getName(), clonedExpr);
        return copy;
    }

    /**
     * @see org.teiid.query.sql.symbol.Symbol#hashCode()
     */
    public int hashCode() {
        if (expression != null) {
            return expression.hashCode();
        }
        return super.hashCode();
    }

    /**
     * ExpressionSymbol matching is not based upon the name
     *
     * @see org.teiid.query.sql.symbol.Symbol#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof ExpressionSymbol)) {
            return false;
        }

        ExpressionSymbol exprSymbol = (ExpressionSymbol)obj;

        if (expression == null ) {
            return exprSymbol.getExpression() == null;
        }

        return expression.equals(exprSymbol.getExpression());
    }

}
