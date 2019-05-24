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

import org.teiid.query.sql.lang.PredicateCriteria.Negatable;
import org.teiid.query.sql.symbol.Expression;

/**
 * This is an abstract class to define some common functionality in the two varieties of
 * IN criteria:  {@link SetCriteria} (where values are specified) and {@link SubquerySetCriteria}
 * (where a subquery is defined and will supply the values for the IN set).
 */
public abstract class AbstractSetCriteria extends PredicateCriteria implements Negatable {

    /** The left expression */
    private Expression expression;

    /** Negation flag. Indicates whether the criteria expression contains a NOT. */
    private boolean negated = false;

    /**
     * Constructor for AbstractSetCriteria.
     */
    protected AbstractSetCriteria() {
        super();
    }

    /**
     * Gets the membership expression to be compared.
     * @return The membership expression
     */
    public Expression getExpression() {
        return this.expression;
    }

    /**
     * Sets the membership expression
     * @param expression The membership expression
     */
    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    /**
     * Returns whether this criteria is negated.
     * @return flag indicating whether this criteria contains a NOT
     */
    public boolean isNegated() {
        return negated;
    }

    /**
     * Sets the negation flag for this criteria.
     * @param negationFlag true if this criteria contains a NOT; false otherwise
     */
    public void setNegated(boolean negationFlag) {
        negated = negationFlag;
    }

    @Override
    public void negate() {
        this.negated = !this.negated;
    }

    /**
     * Deep copy of object
     * @return Deep copy of object
     */
    public abstract Object clone();
}
