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

package com.metamatrix.query.sql.lang;

import java.util.concurrent.atomic.AtomicInteger;

import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.ContextReference;
import com.metamatrix.query.sql.symbol.Expression;

/**
 * This predicate criteria implements the "exists" predicate, which has
 * a subquery in it.  For example,
 * "EXISTS (Select EmployeeID FROM Employees WHERE EmployeeName = 'Smith')".
 */
public class ExistsCriteria extends PredicateCriteria
implements SubqueryContainer, ContextReference {
	
	private static AtomicInteger ID = new AtomicInteger();

    private Command command;
    private String id = "$ec/id" + ID.getAndIncrement(); //$NON-NLS-1$

    /**
     * Default constructor
     */
    public ExistsCriteria() {
        super();
    }

    public ExistsCriteria(Command subqueryCommand){
        this.command = subqueryCommand;
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
     * @see com.metamatrix.query.sql.lang.SubqueryCriteria#getCommand()
     */
    public Command getCommand() {
        return this.command;
    }

    public void setCommand(Command subqueryCommand){
        this.command = subqueryCommand;
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

        if(!(obj instanceof ExistsCriteria)) {
            return false;
        }

        return EquivalenceUtil.areEqual(getCommand(), ((ExistsCriteria)obj).getCommand());
    }

    /**
     * Deep copy of object.  The values Iterator of this object
     * will not be cloned - it will be null in the new object
     * (see #setValueIterator setValueIterator}).
     * @return Deep copy of object
     * @see java.lang.Object#clone()
     */
    public Object clone() {

        Command copyCommand = null;
        if(getCommand() != null) {
            copyCommand = (Command) getCommand().clone();
        }

        return new ExistsCriteria(copyCommand);
    }
}
