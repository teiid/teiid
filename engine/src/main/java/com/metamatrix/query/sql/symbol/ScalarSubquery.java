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

package com.metamatrix.query.sql.symbol;

import java.util.concurrent.atomic.AtomicInteger;

import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

/**
 * This is an Expression implementation that can be used in a SELECT clause.
 * It has a subquery Command which must only produce exactly one
 * value (or an Exception will result during query processing). It's type
 * will be the type of the one symbol to be produced. In theory an instance
 * of this could be used wherever an Expression is legal, but it is
 * specifically needed for the SELECT clause.
 */
public class ScalarSubquery implements Expression, SubqueryContainer, ContextReference {

	private static AtomicInteger ID = new AtomicInteger();
	
    private Command command;
    private Class type;
    private int hashCode;
    private String id = "$sc/id" + ID.getAndIncrement(); //$NON-NLS-1$

    /**
     * Default constructor
     */
    ScalarSubquery() {
        super();
    }

    public ScalarSubquery(Command subqueryCommand){
        this.setCommand(subqueryCommand);
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
