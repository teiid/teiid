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

import java.util.Objects;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;


/**
 * <p>A criteria which represents a simple operator relationship between two expressions.
 * There are 6 operator types.  Each side of the comparison may be an expression, which
 * could be an element, a constant, or a function.
 *
 * <p>Some examples are:
 * <UL>
 * <LI>ticker = 'IBM'</LI>
 * <li>price &gt;= 50</LI>
 * <LI>revenue &lt; 0</LI>
 * <LI>5 &lt;= length(companyName)</LI>
 * </UL>
 */
public class CompareCriteria extends AbstractCompareCriteria implements BinaryComparison {

    /** The right-hand expression. */
    private Expression rightExpression;

    //null means existing, but implied
    //true means completely derived
    //false means required
    private Boolean isOptional = Boolean.FALSE;

    /**
     * Constructs a default instance of this class.
     */
    public CompareCriteria() {
        super();
    }

    /**
     * Constructs an instance of this class for a specific "operand operator
     * operand" clause.
     *
     * @param leftExpression The variable being compared
     * @param rightExpression The value the variable is being compared to (literal or variable)
     * @param operator The operator representing how the variable and value are to
     *                 be compared
     */
    public CompareCriteria( Expression leftExpression, int operator, Expression rightExpression ) {
        super();
        set(leftExpression, operator, rightExpression);
    }

    /**
     * Set right expression.
     * @param expression Right expression
     */
    @Override
    public void setRightExpression(Expression expression) {
        if (!Boolean.FALSE.equals(isOptional) && !Objects.equals(getRightExpression(), expression)) {
            isOptional = false; //isOptional is preserved by clone, reset if the predicate changes
        }
        this.rightExpression = expression;
    }

    @Override
    public void setLeftExpression(Expression expression) {
        if (!Boolean.FALSE.equals(isOptional) && !Objects.equals(getLeftExpression(), expression)) {
            isOptional = false; //isOptional is preserved by clone, reset if the predicate changes
        }
        super.setLeftExpression(expression);
    }

    /**
     * Get right expression.
     * @return right expression
     */
    public Expression getRightExpression() {
        return this.rightExpression;
    }

    /**
     * Sets the operands and operator.  The clause is of the form: &lt;variable&gt; &lt;operator&gt; &lt;value&gt;.
     *
     * @param leftExpression The left expression
     * @param operator The operator representing how the expressions are compared
     * @param rightExpression The right expression
     */
    public void set( Expression leftExpression, int operator, Expression rightExpression ) {
        setLeftExpression(leftExpression);
        setOperator(operator);
        setRightExpression(rightExpression);
    }

    /**
     * Set during planning to indicate that this criteria is no longer needed
     * to correctly process a join
     * @param isOptional
     */
    public void setOptional(Boolean isOptional) {
        if (isOptional == null && Boolean.TRUE.equals(this.isOptional)) {
            return;
        }
        this.isOptional = isOptional;
    }

    /**
     * Returns true if the compare criteria is used as join criteria, but not needed
     * during processing.
     * @return
     */
    public boolean isOptional() {
        return isOptional == null || isOptional;
    }

    public Boolean getIsOptional() {
        return isOptional;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Get hash code.  WARNING: The hash code is based on data in the criteria.
     * If data values are changed, the hash code will change - don't hash this
     * object and change values.
     * @return Hash code
     */
    public int hashCode() {
        int hc = 0;
        hc = HashCodeUtil.hashCode(hc, getOperator());
        //allow for semantic equivalence
        hc += HashCodeUtil.hashCode(0, getLeftExpression());
        hc += HashCodeUtil.hashCode(0, getRightExpression());
        return hc;
    }

    /**
     * Override equals() method.
     * @param obj Other object
     * @return true if objects are equivalent
     */
    public boolean equals(Object obj) {
        // Use super.equals() to check obvious stuff and variable
        if(obj == this) {
            return true;
        }

        if(! (obj instanceof CompareCriteria)) {
            return false;
        }

        //also allow for semantic equivalence on equality
        CompareCriteria cc = (CompareCriteria)obj;
        return getOperator() == cc.getOperator() &&
               (EquivalenceUtil.areEqual(getLeftExpression(), cc.getLeftExpression()) &&
               EquivalenceUtil.areEqual(getRightExpression(), cc.getRightExpression())) || (getOperator() == EQ && EquivalenceUtil.areEqual(getLeftExpression(), cc.getRightExpression()) &&
                       EquivalenceUtil.areEqual(getRightExpression(), cc.getLeftExpression()));
    }

    /**
     * Deep copy of object
     * @return Deep copy of object
     */
    public Object clone() {
        Expression leftCopy = null;
        if(getLeftExpression() != null) {
            leftCopy = (Expression) getLeftExpression().clone();
        }
        Expression rightCopy = null;
        if(getRightExpression() != null) {
            rightCopy = (Expression) getRightExpression().clone();
        }

        CompareCriteria result = new CompareCriteria(leftCopy, getOperator(), rightCopy);
        result.isOptional = isOptional;
        return result;
    }

    public int getReverseOperator() {
        int operator = getOperator();
        switch(operator) {
        case CompareCriteria.LT:    return CompareCriteria.GT;
        case CompareCriteria.LE:    return CompareCriteria.GE;
        case CompareCriteria.GT:    return CompareCriteria.LT;
        case CompareCriteria.GE:    return CompareCriteria.LE;
        }
        return operator;
    }

}  // END CLASS
