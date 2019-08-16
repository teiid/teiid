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
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.PredicateCriteria.Negatable;
import org.teiid.query.sql.symbol.Expression;


/**
 * Represents criteria such as:  "&lt;expression&gt; [NOT] BETWEEN &lt;lowerExpression&gt; AND &lt;upperExpression&gt;".
 */
public class BetweenCriteria extends PredicateCriteria implements Negatable {

    private Expression expression;
    private Expression lowerExpression;
    private Expression upperExpression;

    /** Negation flag. Indicates whether the criteria expression contains a NOT. */
    private boolean negated = false;

    /**
     * Constructs a default instance of this class.
     */
    public BetweenCriteria() {}

    /**
     * Constructs an instance of this class with an expression
     * @param expression The expression to be compared to null
     */
    public BetweenCriteria(Expression expression,
                            Expression lowerExpression,
                            Expression upperExpression ) {
        this.expression = expression;
        this.lowerExpression = lowerExpression;
        this.upperExpression = upperExpression;
    }

    /**
     * Set expression.
     * @param expression Expression to compare to the upper and lower values
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
     * Set the lower expression.
     * @param lowerExpression the lower expression
     */
    public void setLowerExpression(Expression lowerExpression) {
        this.lowerExpression = lowerExpression;
    }

    /**
     * Get the lower expression.
     * @return the lower expression
     */
    public Expression getLowerExpression() {
        return this.lowerExpression;
    }

    /**
     * Set the upper expression.
     * @param upperExpression the upper expression
     */
    public void setUpperExpression(Expression upperExpression) {
        this.upperExpression = upperExpression;
    }

    /**
     * Get the upper expression.
     * @return the upper expression
     */
    public Expression getUpperExpression() {
        return this.upperExpression;
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

    /**
     * Method for accepting a visitor.  It is the responsibility of the
     * language object to use the visitor's iterator to call acceptVisitor
     * on the "next" object, according to the iteration strategy.
     * @param visitor Visitor being used
     */
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
        int hc = 0;
        hc = HashCodeUtil.hashCode(hc, getExpression());
        hc = HashCodeUtil.hashCode(hc, getLowerExpression());
        hc = HashCodeUtil.hashCode(hc, getUpperExpression());
        return hc;
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

        if(!(obj instanceof BetweenCriteria)) {
            return false;
        }

        BetweenCriteria other = (BetweenCriteria) obj;
        if (isNegated() ^ other.isNegated()) {
            return false;
        }
        return  EquivalenceUtil.areEqual(getExpression(), other.getExpression()) &&
                EquivalenceUtil.areEqual(getLowerExpression(), other.getLowerExpression()) &&
                EquivalenceUtil.areEqual(getUpperExpression(), other.getUpperExpression());

    }

    /**
     * Deep copy of object
     * @return Deep copy of object
     */
    public Object clone() {
        Expression copy = null;
        Expression lowerCopy = null;
        Expression upperCopy = null;
        if(getExpression() != null) {
            copy = (Expression) getExpression().clone();
        }
        if(getLowerExpression() != null) {
            lowerCopy = (Expression) getLowerExpression().clone();
        }
        if(getUpperExpression() != null) {
            upperCopy = (Expression) getUpperExpression().clone();
        }
        BetweenCriteria criteriaCopy = new BetweenCriteria(copy, lowerCopy, upperCopy);
        criteriaCopy.setNegated(isNegated());
        return criteriaCopy;
    }

    @Override
    public void negate() {
        this.negated = !this.negated;
    }

}

