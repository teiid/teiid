/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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
