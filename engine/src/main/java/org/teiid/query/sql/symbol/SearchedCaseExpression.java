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
