/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.sql.lang;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * A criteria which is true is the expression's value is a member in a list
 * of values returned from a subquery.  This criteria can be represented as
 * "<expression> IN (SELECT ...)".
 */
public class SubquerySetCriteria extends AbstractSetCriteria implements SubqueryContainer {

    private Command command;
	private ValueIterator valueIterator;

    /**
     * Constructor for SubquerySetCriteria.
     */
    public SubquerySetCriteria() {
        super();
    }

    public SubquerySetCriteria(Expression expression, Command subCommand) {
        setExpression(expression);
        setCommand(subCommand);
    }

    /**
     * Set the subquery command (either a SELECT or a procedure execution).
     * @param command Command to execute to get the values for the criteria
     */
    public void setCommand(Command command) {
        this.command = command;
    }

    /**
     * Get the subquery command used to produce the values for this SetCriteria.
     * @return Command Command to execute
     */
    public Command getCommand() {
        return this.command;
    }

	/**
	 * Returns always the same instance of a ValueIterator, but
	 * {@link ValueIterator#reset resets} it each time this method is called
	 * @return this object's ValueIterator instance (always the same instance)
     * @throws MetaMatrixRuntimeException if the subquery for this set criteria
     * has not yet been processed and no value iterator is available
	 * @see com.metamatrix.query.sql.lang.AbstractSetCriteria#getValueIterator()
	 */
	public ValueIterator getValueIterator() {
        if (this.valueIterator == null){
            throw new MetaMatrixRuntimeException(ErrorMessageKeys.SQL_0012, QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0012));
        }
        this.valueIterator.reset();
		return this.valueIterator;
	}

	/**
	 * Set the ValueIterator on this object (the ValueIterator will encapsulate
	 * the single-column results of the subquery processor plan).  This
	 * ValueIterator must be set before processing (before the Criteria can be
	 * evaluated).  Also, this ValueIterator should be considered transient -
	 * only available during processing - and it will not be cloned should
	 * this Criteria object be cloned.
	 * @param valueIterator encapsulating the results of the sub query
	 */
	public void setValueIterator(ValueIterator valueIterator) {
		this.valueIterator = valueIterator;
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
        hc = HashCodeUtil.hashCode(hc, getExpression());
        hc = HashCodeUtil.hashCode(hc, getCommand());
        return hc;
    }

    /**
     * Override equals() method.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Use super.equals() to check obvious stuff and variable
        if(obj == this) {
            return true;
        }

        if(! (obj instanceof SubquerySetCriteria)) {
            return false;
        }

        SubquerySetCriteria sc = (SubquerySetCriteria)obj;
        if (isNegated() ^ sc.isNegated()) {
            return false;
        }

        return EquivalenceUtil.areEqual(getExpression(), sc.getExpression()) &&
               EquivalenceUtil.areEqual(getCommand(), sc.getCommand());
    }

    /**
     * Deep copy of object.  The values Iterator of this object
     * will not be cloned - it will be null in the new object
     * (see #setValueIterator setValueIterator}).
     * @return Deep copy of object
     */
    public Object clone() {
        Expression copy = null;
        if(getExpression() != null) {
            copy = (Expression) getExpression().clone();
        }

        Command copyCommand = null;
        if(getCommand() != null) {
            copyCommand = (Command) getCommand().clone();
        }

        SubquerySetCriteria criteriaCopy = new SubquerySetCriteria(copy, copyCommand);
        criteriaCopy.setNegated(isNegated());
        return criteriaCopy;
    }

}
