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

import org.teiid.query.sql.*;

/**
 * <p>This is a subclass of Symbol representing an expression in the SELECT clause.  The
 * expression may be a constant, function, or scalar subquery.  The name of this symbol is always generated
 * and typically should not be displayed.  If necessary, the ExpressionSymbol may be 
 * wrapped by an AliasSymbol to register the name in a query.  The definition of the 
 * symbol is the functional expression.  Resolution will produce a list of groups and 
 * elements used by the expression.</p>
 */
public class ExpressionSymbol extends SingleElementSymbol {
	private Expression expression;

    /**
     * Constructor used for cloning 
     * @param name
     * @param canonicalName
     * @since 4.3
     */
    protected ExpressionSymbol(String name, String canonicalName, Expression expression) {
        super(name, canonicalName);
        this.expression = expression;
    }
    
    /**
     * Construct an ExpressionSymbol with name and expression.
     */
    public ExpressionSymbol(String name, Expression expression) {
        super(name);
		this.expression = expression;
    }

	/**
	 * Get the expression for this symbol
	 * @return Expression for this symbol
	 */
	public Expression getExpression() {
		return this.expression;
	}
	
	/**
  	 * Set the expression represented by this symbol.
  	 * @param expression Expression for this expression symbol
 	 */
	public void setExpression(Expression expression) {
		this.expression = expression;
	}

	/**
	 * Get the type of the symbol
	 * @return Type of the symbol, may be null before resolution
	 */
	public Class getType() {
		return this.expression.getType();
	}	
	
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * If elementSymbols is not null return true, else return false
     * @return boolean True if expression symbol has been resolved to element symbols
     */
    public boolean isResolved() {
        return true;
    }

	/**
	 * Return a deep copy of this object
	 * @return Deep copy of this object
	 */
	public Object clone() {
	    Expression clonedExpr = null;
	    if(getExpression() != null) { 
			clonedExpr = (Expression) getExpression().clone();		
	    }
	    ExpressionSymbol copy = new ExpressionSymbol(getName(), getCanonical(), clonedExpr);
	    return copy;
	}

    /** 
     * @see org.teiid.query.sql.symbol.Symbol#hashCode()
     */
    public int hashCode() {
        if (expression != null) {
            return expression.hashCode();
        }
        return super.hashCode();
    }
    
    /** 
     * ExpressionSymbol matching is not based upon the name
     * 
     * @see org.teiid.query.sql.symbol.Symbol#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (!(obj instanceof ExpressionSymbol)) {
            return false;
        }
        
        ExpressionSymbol exprSymbol = (ExpressionSymbol)obj;
        
        if (expression == null ) {
            return exprSymbol.getExpression() == null;
        }
        
        return expression.equals(exprSymbol.getExpression());
    }
        	
}
