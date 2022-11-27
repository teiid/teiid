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

import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.ExistsCriteria.SubqueryHint;
import org.teiid.query.sql.symbol.ContextReference;
import org.teiid.query.sql.symbol.Expression;


/**
 * <p>This class implements a quantified comparison predicate.  This is
 * a criteria which represents a simple operator relationship between an expression and
 * either a scalar subquery or a table subquery preceded by one of the possible quantifiers.
 *
 *
 * <p>The quantifiers are:
 * <ul>
 * <li>{@link #SOME} and {@link #ANY}, which are synonymous - the criteria is true if there is at
 * least one comparison between the left expression and the values of the subquery.  The criteria
 * is false if the subquery returns no rows.</li>
 * <li>{@link #ALL} - the criteria is true only if all of the comparisons between the left
 * expression and each value of the subquery is true.  The criteria is also true if the subquery
 * returns no rows.</li></ul>
 *
 * <p>Some examples are:
 * <UL>
 * <LI>ticker = ANY (Select ... FROM ... WHERE ... )</LI>
 * <li>price &gt;= ALL (Select ... FROM ... WHERE ... )</LI>
 * <LI>revenue &lt; (Select ... FROM ... WHERE ... )</LI>
 * </UL>
 *
 * This can also represent a quantified comparison against array.  In which case the
 * arrayExpression member will be set and command will not.
 *
 */
public class SubqueryCompareCriteria extends AbstractCompareCriteria
implements SubqueryContainer<QueryCommand>, ContextReference {

    private static AtomicInteger ID = new AtomicInteger();

    /** "Some" predicate quantifier (equivalent to "Any") */
    public static final int SOME = 2;

    /** "Any" predicate quantifier (equivalent to "Some") */
    public static final int ANY = 3;

    /** "All" predicate quantifier */
    public static final int ALL = 4;

    private int predicateQuantifier = ALL;

    private QueryCommand command;
    private Expression arrayExpression;
    private String id = "$scc/id" + ID.getAndIncrement(); //$NON-NLS-1$

    private SubqueryHint subqueryHint = new SubqueryHint();

    public SubqueryCompareCriteria(){
        super();
    }

    public SubqueryCompareCriteria(Expression leftExpression, QueryCommand subCommand, int operator, int predicateQuantifier) {
        setLeftExpression(leftExpression);
        setCommand(subCommand);
        setOperator(operator);
        setPredicateQuantifier(predicateQuantifier);
    }

    @Override
    public String getContextSymbol() {
        return id;
    }

    /**
     * Get the predicate quantifier - returns one of the following:
     * <ul>
     * <li>{@link #ANY}</li>
     * <li>{@link #SOME}</li>
     * <li>{@link #ALL}</li></ul>
     * @return the predicate quantifier
     */
    public int getPredicateQuantifier() {
        return this.predicateQuantifier;
    }

    /**
     * Set the predicate quantifier - use one of the following:
     * <ul>
     * <li>{@link #ANY}</li>
     * <li>{@link #SOME}</li>
     * <li>{@link #ALL}</li></ul>
     * @param predicateQuantifier the predicate quantifier
     */
    public void setPredicateQuantifier(int predicateQuantifier) {
        this.predicateQuantifier = predicateQuantifier;
    }

    public QueryCommand getCommand() {
        return this.command;
    }

    /**
     * Set the subquery command (either a SELECT or a procedure execution).
     * @param command Command to execute to get the values for the criteria
     */
    public void setCommand(QueryCommand command) {
        this.command = command;
    }

    /**
     * Returns the predicate quantifier as a string.
     * @return String version of predicate quantifier
     */
    public String getPredicateQuantifierAsString() {
        switch ( this.predicateQuantifier ) {
            case ANY: return "ANY"; //$NON-NLS-1$
            case SOME: return "SOME"; //$NON-NLS-1$
            case ALL: return "ALL"; //$NON-NLS-1$
            default: return "??"; //$NON-NLS-1$
        }
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
        hc = HashCodeUtil.hashCode(hc, getLeftExpression());
        hc = HashCodeUtil.hashCode(hc, getOperator());
        hc = HashCodeUtil.hashCode(hc, getPredicateQuantifier());
        hc = HashCodeUtil.hashCode(hc, getCommand());
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

        if(! (obj instanceof SubqueryCompareCriteria)) {
            return false;
        }

        SubqueryCompareCriteria scc = (SubqueryCompareCriteria)obj;

        return getOperator() == scc.getOperator() &&
               getPredicateQuantifier() == scc.getPredicateQuantifier() &&
               EquivalenceUtil.areEqual(getLeftExpression(), scc.getLeftExpression()) &&
               EquivalenceUtil.areEqual(getCommand(), scc.getCommand()) &&
               EquivalenceUtil.areEqual(subqueryHint, scc.subqueryHint) &&
               EquivalenceUtil.areEqual(arrayExpression, scc.arrayExpression);
    }

    /**
     * Deep copy of object.  The values Iterator of this object
     * will not be cloned - it will be null in the new object
     * (see #setValueIterator setValueIterator}).
     * @return Deep copy of object
     * @see java.lang.Object#clone()
     */
    public Object clone() {
        Expression leftCopy = null;
        if(getLeftExpression() != null) {
            leftCopy = (Expression) getLeftExpression().clone();
        }

        QueryCommand copyCommand = null;
        if(getCommand() != null) {
            copyCommand = (QueryCommand) getCommand().clone();
        }

        SubqueryCompareCriteria copy = new SubqueryCompareCriteria(leftCopy, copyCommand, this.getOperator(), this.getPredicateQuantifier());
        if (this.subqueryHint != null) {
            copy.subqueryHint = this.subqueryHint.clone();
        }
        if (this.arrayExpression != null) {
            copy.arrayExpression = (Expression) this.arrayExpression.clone();
        }
        return copy;
    }

    @Override
    public void negate() {
        super.negate();
        if (this.predicateQuantifier == ALL) {
            this.predicateQuantifier = SOME;
        } else {
            this.predicateQuantifier = ALL;
        }
    }

    public SubqueryHint getSubqueryHint() {
        return subqueryHint;
    }

    public void setSubqueryHint(SubqueryHint subqueryHint) {
        this.subqueryHint = subqueryHint;
    }

    public Expression getArrayExpression() {
        return arrayExpression;
    }

    public void setArrayExpression(Expression expression) {
        this.arrayExpression = expression;
    }

}
