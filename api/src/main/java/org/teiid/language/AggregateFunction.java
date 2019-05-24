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

package org.teiid.language;

import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents an aggregate function.
 */
public class AggregateFunction extends Function {

    public static final String COUNT = "COUNT"; //$NON-NLS-1$
    public static final String AVG = "AVG"; //$NON-NLS-1$
    public static final String SUM = "SUM"; //$NON-NLS-1$
    public static final String MIN = "MIN"; //$NON-NLS-1$
    public static final String MAX = "MAX";     //$NON-NLS-1$
    public static final String STDDEV_POP = "STDDEV_POP"; //$NON-NLS-1$
    public static final String STDDEV_SAMP = "STDDEV_SAMP"; //$NON-NLS-1$
    public static final String VAR_SAMP = "VAR_SAMP"; //$NON-NLS-1$
    public static final String VAR_POP = "VAR_POP"; //$NON-NLS-1$

    private String aggName;
    private boolean isDistinct;
    private Expression condition;
    private OrderBy orderBy;

    public AggregateFunction(String aggName, boolean isDistinct, List<? extends Expression> params, Class<?> type) {
        super(aggName, params, type);
        this.aggName = aggName;
        this.isDistinct = isDistinct;
    }

    /**
     * Get the name of the aggregate function.  This will be one of the constants defined
     * in this class.
     */
    public String getName() {
        return this.aggName;
    }

    /**
     * Determine whether this function was executed with DISTINCT.  Executing
     * with DISTINCT will remove all duplicate values in a group when evaluating
     * the aggregate function.
     * @return True if DISTINCT mode is used
     */
    public boolean isDistinct() {
        return this.isDistinct;
    }

    /**
     * Get the expression within the aggregate function.  The expression will be
     * null for the special case COUNT(*).  This is the only case where the
     * expression will be null
     * Only valid for 0/1 ary aggregates
     * @return The expression or null for COUNT(*)
     * @deprecated
     */
    public Expression getExpression() {
        if (this.getParameters().isEmpty()) {
            return null;
        }
        return this.getParameters().get(0);
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Set the name of the aggregate function.  This will be one of the constants defined
     * in this class.
     * @param name New aggregate function name
     */
    public void setName(String name) {
        this.aggName = name;
    }

    /**
     * Set whether this function was executed with DISTINCT.  Executing
     * with DISTINCT will remove all duplicate values in a group when evaluating
     * the aggregate function.
     * @param isDistinct True if DISTINCT mode should be used
     */
    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    /**
     *
     * @return the filter clause condition
     */
    public Expression getCondition() {
        return condition;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public OrderBy getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }

}
