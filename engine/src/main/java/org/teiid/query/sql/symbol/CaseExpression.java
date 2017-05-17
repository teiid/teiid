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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.LanguageVisitor;


public class CaseExpression extends AbstractCaseExpression {

    /**
     * The expression whose evaluation is being tested in this case expression.
     */
    private Expression expression = null;

    /**
     * Ordered List of Expressions in the WHEN parts of this expression.
     */
    private List when = null;

    /**
     * Constructor for CaseExpression objects
     * @param expression a non-null expression
     * @param when a non-null List containing at least one Expression
     * @param then a non-null List containing at least one Expression
     */
    public CaseExpression(Expression expression, List when, List then) {
        setExpression(expression);
        setWhen(when, then);
    }

    /**
     * Gets the expression whose evaluation is being tested in this case expression.
     * @return
     */
    public Expression getExpression() {
        return expression;
    }

    /**
     * Sets the expression for this case expression
     * @param expr a non-null Expression
     */
    public void setExpression(Expression expr) {
        if (expr == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0035")); //$NON-NLS-1$
        }
        this.expression = expr;
    }

    /**
     *
     * @see org.teiid.query.sql.symbol.AbstractCaseExpression#getWhenCount()
     */
    public int getWhenCount() {
        return (when == null) ? 0 : when.size();
    }

    /**
     * Gets the List of Expressions in the WHEN parts of this expression. Never null.
     * @return
     */
    public List getWhen() {
        return when;
    }

    /**
     * Gets the WHEN expression at the given 0-based index.
     * @param index
     * @return
     */
    public Expression getWhenExpression(int index) {
        return (Expression)when.get(index);
    }

    /**
     * Sets the WHEN and THEN parts of this CASE expression.
     * Both lists should have the same number of Expressions.
     * @param when a non-null List of at least one Expression
     * @param then a non-null List of at least one Expression
     */
    public void setWhen(List when, List then) {
        if (when == null || then == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0036")); //$NON-NLS-1$
        }
        if (when.size() != then.size() ||
            when.size() < 1) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0036")); //$NON-NLS-1$
        }
        for (int i = 0 ; i < when.size(); i++) {
            if (!(when.get(i) instanceof Expression)) {
                throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0037")); //$NON-NLS-1$
            }
            if (!(then.get(i) instanceof Expression)) {
                throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0038")); //$NON-NLS-1$
            }
        }
        if (this.when != when) {
            this.when = Collections.unmodifiableList(when);
        }
        setThen(then);
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @see java.lang.Object#clone()
     */
    public Object clone() {
        Expression expr = (Expression)expression.clone();
        int whenCount = getWhenCount();
        ArrayList whenCopy = new ArrayList(whenCount);
        ArrayList thenCopy = new ArrayList(whenCount);
        for(int i = 0; i < whenCount; i++) {
            whenCopy.add(getWhenExpression(i).clone());
            thenCopy.add(getThenExpression(i).clone());
        }
        Expression elseExpr = getElseExpression();
        if (elseExpr != null) {
            elseExpr = (Expression)elseExpr.clone();
        }

        CaseExpression copy = new CaseExpression(expr, whenCopy, thenCopy);
        copy.setType(getType());
        copy.setElseExpression(elseExpr);
        return copy;
    }

    public boolean equals(Object obj) {
        if (!super.equals(obj)) return false;
        if (!(obj instanceof CaseExpression)) return false;
        CaseExpression other = (CaseExpression)obj;
        return getExpression().equals(other.getExpression()) &&
               when.equals(other.when);
    }
}
