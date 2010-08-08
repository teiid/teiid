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

package org.teiid.query.sql.proc;

import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;


/**
 * <p> This class represents a error assignment statement in the storedprocedure language.
 * It extends the <code>Statement</code> that could part of a <code>Block</code>.  This
 * this object holds and error message.</p>
 */
public class RaiseErrorStatement extends Statement implements ExpressionStatement {
	
	private Expression expression;

	/**
	 * Constructor for RaiseErrorStatement.
	 */
	public RaiseErrorStatement() {
		super();
	}
	
	/**
	 * Constructor for RaiseErrorStatement.
	 * @param message The error message
	 */
	public RaiseErrorStatement(Expression message) {
		expression = message;
	}
        
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
    
    public Expression getExpression() {
		return expression;
	}
    
    public void setExpression(Expression expression) {
		this.expression = expression;
	}
    
    /** 
     * @see org.teiid.query.sql.proc.AssignmentStatement#getType()
     */
    public int getType() {
        return TYPE_ERROR;
    }

	@Override
	public RaiseErrorStatement clone() {
		return new RaiseErrorStatement((Expression) this.expression.clone());
	}
	
	@Override
	public int hashCode() {
		return expression.hashCode();
	}
	
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		
		if (!(obj instanceof RaiseErrorStatement)) {
			return false;
		}
		
		RaiseErrorStatement other = (RaiseErrorStatement)obj;
		
		return other.expression.equals(this.expression);
	}
	
	@Override
	public Class<?> getExpectedType() {
		return DataTypeManager.DefaultDataClasses.STRING;
	}
    
} // END CLASS