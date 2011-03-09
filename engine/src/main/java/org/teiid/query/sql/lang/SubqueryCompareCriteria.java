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

package org.teiid.query.sql.lang;

import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.ContextReference;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ScalarSubquery;


/**
 * <p>This class implements a quantified comparison predicate.  This is
 * a criteria which represents a simple operator relationship between an expression and
 * either a scalar subquery or a table subquery preceded by one of the possible quantifiers.
 * </p>
 *
 * <p>The quantifiers are:
 * <ul><li>{@link #NO_QUANTIFIER}, meaning the subquery has no quantifier and therefore must be
 * a scalar subquery</li>
 * <li>{@link #SOME} and {@link #ANY}, which are synonymous - the criteria is true if there is at
 * least one comparison between the left expression and the values of the subquery.  The criteria
 * is false if the subquery returns no rows.</li>
 * <li>{@link #ALL}</li> - the criteria is true only if all of the comparisons between the left
 * expression and each value of the subquery is true.  The criteria is also true if the subquery
 * returns no rows.</li></ul>
 *
 * <p>Some examples are:</p>
 * <UL>
 * <LI>ticker = ANY (Select ... FROM ... WHERE ... )</LI>
 * <li>price &gt;= ALL (Select ... FROM ... WHERE ... )</LI>
 * <LI>revenue &lt; (Select ... FROM ... WHERE ... )</LI>
 * </UL>
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
    private String id = "$scc/id" + ID.getAndIncrement(); //$NON-NLS-1$

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
    
    @Override
    public Expression getValueExpression() {
    	return null;
    }

    /**
     * Get the predicate quantifier - returns one of the following:
     * <ul><li>{@link #NO_QUANTIFIER}</li>
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
     * <ul><li>{@link #NO_QUANTIFIER}</li>
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
            case ANY: return "ANY "; //$NON-NLS-1$
            case SOME: return "SOME "; //$NON-NLS-1$
            case ALL: return "ALL "; //$NON-NLS-1$
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
               EquivalenceUtil.areEqual(getCommand(), scc.getCommand());
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

        return new SubqueryCompareCriteria(leftCopy, copyCommand, this.getOperator(), this.getPredicateQuantifier());
    }

    /** 
     * @see org.teiid.query.sql.lang.AbstractCompareCriteria#getRightExpression()
     */
    public Expression getRightExpression() {
        return new ScalarSubquery(getCommand());
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

}
