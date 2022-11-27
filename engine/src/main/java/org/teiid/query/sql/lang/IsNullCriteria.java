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

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.*;
import org.teiid.query.sql.lang.PredicateCriteria.Negatable;
import org.teiid.query.sql.symbol.Expression;


/**
 * Represents criteria such as:  "&lt;expression&gt; IS NULL".
 */
public class IsNullCriteria extends PredicateCriteria implements Negatable {

    private Expression expression;
    /** Negation flag. Indicates whether the criteria expression contains a NOT. */
    private boolean negated;

    /**
     * Constructs a default instance of this class.
     */
    public IsNullCriteria() {}

    /**
     * Constructs an instance of this class with an expression
     * @param expression The expression to be compared to null
     */
    public IsNullCriteria( Expression expression ) {
        this.expression = expression;
    }

    /**
     * Set expression.
     * @param expression Expression to compare to null
     */
    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    /**
     * Get expression.
     * @return Expression to compare
     */
    public Expression getExpression() {
        return this.expression;
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

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Get hash code.  WARNING: The hash code is based on data in the criteria.
     * If data values are changed, the hash code will change - don't hash this
     * object and change values.
     * @return Hash code for object
     */
    public int hashCode() {
        return (getExpression() == null) ? 0 : getExpression().hashCode();
    }

    /**
     * Comparees this criteria to another object for equality
     * @param obj Other object
     * @return True if objects are equal
     */
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(! (obj instanceof IsNullCriteria)) {
            return false;
        }

        IsNullCriteria other = (IsNullCriteria) obj;
        if (isNegated() ^ other.isNegated()) {
            return false;
        }
        return EquivalenceUtil.areEqual(getExpression(), other.getExpression());
    }

    /**
     * Deep copy of object
     * @return Deep copy of object
     */
    public Object clone() {
        Expression copy = null;
        if(getExpression() != null) {
            copy = (Expression) getExpression().clone();
        }
        IsNullCriteria criteriaCopy = new IsNullCriteria(copy);
        criteriaCopy.setNegated(isNegated());
        return criteriaCopy;
    }


}