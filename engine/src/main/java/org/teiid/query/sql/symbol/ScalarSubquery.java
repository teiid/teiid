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

package org.teiid.query.sql.symbol;

import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * This is an Expression implementation that can be used in a SELECT clause.
 * It has a subquery Command which must only produce exactly one
 * value (or an Exception will result during query processing). It's type
 * will be the type of the one symbol to be produced. In theory an instance
 * of this could be used wherever an Expression is legal, but it is
 * specifically needed for the SELECT clause.
 */
public class ScalarSubquery implements Expression, SubqueryContainer<QueryCommand>, ContextReference {

	private static AtomicInteger ID = new AtomicInteger();
	
    private QueryCommand command;
    private Class<?> type;
    private int hashCode;
    private String id = "$sc/id" + ID.getAndIncrement(); //$NON-NLS-1$
    private boolean shouldEvaluate;

    /**
     * Default constructor
     */
    ScalarSubquery() {
        super();
    }

    public ScalarSubquery(QueryCommand subqueryCommand){
        this.setCommand(subqueryCommand);
    }
    
    public boolean shouldEvaluate() {
    	return shouldEvaluate;
    }
    
    public void setShouldEvaluate(boolean shouldEvaluate) {
		this.shouldEvaluate = shouldEvaluate;
	}
    
    @Override
    public String getContextSymbol() {
    	return id;
    }
    
    /**
     * @see org.teiid.query.sql.symbol.Expression#getType()
     */
    public Class<?> getType() {
        if (this.type == null){
            Expression symbol = this.command.getProjectedSymbols().iterator().next();
            this.type = symbol.getType();
        }
        //may still be null if this.command wasn't resolved
        return this.type;
    }

    /**
     * Set type of ScalarSubquery
     * @param type New type
     */
    public void setType(Class<?> type) {
        this.type = type;
    }

    public QueryCommand getCommand() {
        return this.command;
    }

    /**
     * Sets the command.  Also modifies the hash code of this object, so
     * caution should be used in using this method.
     */
    public void setCommand(QueryCommand command){
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
    	QueryCommand copyCommand = null;
        if(getCommand() != null) {
            copyCommand = (QueryCommand) getCommand().clone();
        }
        ScalarSubquery clone = new ScalarSubquery(copyCommand);
        //Don't invoke the lazy-loading getType()
        clone.setType(this.type);
        clone.shouldEvaluate = this.shouldEvaluate;
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
