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
import org.teiid.query.sql.lang.Criteria;


public class SearchedCaseExpression extends AbstractCaseExpression {

    /**
     * Ordered List of Criteria in the WHEN parts of this expression.
     */
    private List when = null;

    /**
     * Constructor for SearchedCaseExpression objects
     * @param when a non-null List containing at least one Criteria
     * @param then a non-null List containing at least one Expression
     */
    public SearchedCaseExpression(List when, List then) {
        setWhen(when, then);
    }

    /**
     *
     * @see org.teiid.query.sql.symbol.AbstractCaseExpression#getWhenCount()
     */
    public int getWhenCount() {
        return (when == null) ? 0 : when.size();
    }

    /**
     * Gets the List of Criteria in the WHEN parts of this expression. Never null.
     * @return
     */
    public List getWhen() {
        return when;
    }

    /**
     * Gets the WHEN criteria at the given 0-based index.
     * @param index
     * @return
     */
    public Criteria getWhenCriteria(int index) {
        return (Criteria)when.get(index);
    }

    /**
     * Sets the WHEN and THEN parts of this CASE expression.
     * Both lists should have the same number of items.
     * @param when a non-null List of at least one Criteria
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
            if (!(when.get(i) instanceof Criteria)) {
                throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0039")); //$NON-NLS-1$
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
        int whenCount = getWhenCount();
        ArrayList whenCopy = new ArrayList(whenCount);
        ArrayList thenCopy = new ArrayList(whenCount);
        for(int i = 0; i < whenCount; i++) {
            whenCopy.add(getWhenCriteria(i).clone());
            thenCopy.add(getThenExpression(i).clone());
        }
        Expression elseExpr = getElseExpression();
        if (elseExpr != null) {
             elseExpr = (Expression)getElseExpression().clone();
        }

        SearchedCaseExpression copy = new SearchedCaseExpression(whenCopy, thenCopy);
        copy.setType(getType());
        copy.setElseExpression(elseExpr);
        return copy;
    }

    public boolean equals(Object obj) {
        if (!super.equals(obj)) return false;
        if (!(obj instanceof SearchedCaseExpression)) return false;
        return when.equals(((SearchedCaseExpression)obj).when);
    }
}
