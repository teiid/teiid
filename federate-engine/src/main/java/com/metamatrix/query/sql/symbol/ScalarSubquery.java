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

package com.metamatrix.query.sql.symbol;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.util.ValueIteratorProvider;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * This is an Expression implementation that can be used in a SELECT clause.
 * It has a subquery Command which must only produce exactly one
 * value (or an Exception will result during query processing). It's type
 * will be the type of the one symbol to be produced. In theory an instance
 * of this could be used wherever an Expression is legal, but it is
 * specifically needed for the SELECT clause.
 */
public class ScalarSubquery implements Expression, SubqueryContainer, ValueIteratorProvider {

    private Command command;
    private Class type;
    private int hashCode;

    // This is "transient" state, available only during query
    // processing.  It is not cloned, if present.
    private ValueIterator valueIterator;

    /**
     * Default constructor
     */
    ScalarSubquery() {
        super();
    }

    public ScalarSubquery(Command subqueryCommand){
        this.setCommand(subqueryCommand);
    }

    /**
     * Returns always the same instance of a ValueIterator, but
     * {@link ValueIterator#reset resets} it each time this method is called
     * @return this object's ValueIterator instance (always the same instance)
     * @throws MetaMatrixRuntimeException if the subquery for this set criteria
     * has not yet been processed and no value iterator is available
     * @see com.metamatrix.query.sql.lang.SubqueryLanguageObject#getValueIterator()
     */
    public ValueIterator getValueIterator() {
        if (this.valueIterator == null){
            throw new MetaMatrixRuntimeException(ErrorMessageKeys.SQL_0034, QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0034));
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

    /**
     * @see com.metamatrix.query.sql.symbol.Expression#isResolved()
     */
    public boolean isResolved() {
        return (this.getType() != null);
    }

    /**
     * @see com.metamatrix.query.sql.symbol.Expression#getType()
     */
    public Class getType() {
        if (this.type == null){
            Expression symbol = (Expression)this.command.getProjectedSymbols().iterator().next();
            this.type = symbol.getType();
        }
        //may still be null if this.command wasn't resolved
        return this.type;
    }

    /**
     * Set type of ScalarSubquery
     * @param type New type
     */
    public void setType(Class type) {
        this.type = type;
    }

    /**
     * @see com.metamatrix.query.sql.lang.SubqueryLanguageObject#getCommand()
     */
    public Command getCommand() {
        return this.command;
    }

    /**
     * Sets the command.  Also modifies the hash code of this object, so
     * caution should be used in using this method.
     * @see com.metamatrix.query.sql.lang.SubqueryLanguageObject#setCommand()
     */
    public void setCommand(Command command){
        this.command = command;
        this.hashCode = command.hashCode();
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Compare this ScalarSubquery to another ScalarSubquery for equality.
     * @param obj Other object
     * @return true if objects are equal
     */
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(! (obj instanceof ScalarSubquery)) {
            return false;
        }
        ScalarSubquery other = (ScalarSubquery) obj;

        return other.getCommand().equals(this.getCommand());
    }

    /**
     * Get hashcode for the object
     * @return Hash code
     */
    public int hashCode() {
        return this.hashCode;
    }

    /**
     * Returns a safe clone
     * @see java.lang.Object#clone()
     */
    public Object clone() {
        Command copyCommand = null;
        if(getCommand() != null) {
            copyCommand = (Command) getCommand().clone();
        }
        ScalarSubquery clone = new ScalarSubquery(copyCommand);
        //Don't invoke the lazy-loading getType()
        clone.setType(this.type);
        return clone;
    }

    /**
     * Returns string representation of this object.
     * @return String representing the object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }

}
