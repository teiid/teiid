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
public class RaiseStatement extends Statement implements ExpressionStatement {
	
	private Expression expression;
	private boolean warning;

	public RaiseStatement() {
	}
	
	/**
	 * Constructor for RaiseErrorStatement.
	 * @param message The error message
	 */
	public RaiseStatement(Expression message) {
		expression = message;
	}
	
	public RaiseStatement(Expression message, boolean warning) {
		expression = message;
		this.warning = warning;
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
    
    public int getType() {
        return TYPE_ERROR;
    }

	@Override
	public RaiseStatement clone() {
		return new RaiseStatement((Expression) this.expression.clone(), warning);
	}
	
	@Override
	public int hashCode() {
		return expression.hashCode();
	}
	
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		
		if (!(obj instanceof RaiseStatement)) {
			return false;
		}
		
		RaiseStatement other = (RaiseStatement)obj;
		
		return other.expression.equals(this.expression) && this.warning == other.warning;
	}
	
	@Override
	public Class<?> getExpectedType() {
		return DataTypeManager.DefaultDataClasses.OBJECT;
	}
	
	public boolean isWarning() {
		return warning;
	}
	
	public void setWarning(boolean warning) {
		this.warning = warning;
	}
    
} // END CLASS