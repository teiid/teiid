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

import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

public class SetClause implements LanguageObject {
	
	private static final long serialVersionUID = 8174681510498719451L;
	
	private ElementSymbol symbol;
	private Expression value;
	
	public SetClause(ElementSymbol symbol, Expression value) {
		ArgCheck.isNotNull(symbol);
		ArgCheck.isNotNull(value);
		this.symbol = symbol;
		this.value = value;
	}

	public ElementSymbol getSymbol() {
		return symbol;
	}

	public void setSymbol(ElementSymbol symbol) {
		this.symbol = symbol;
	}

	public Expression getValue() {
		return value;
	}

	public void setValue(Expression value) {
		this.value = value;
	}

	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
	@Override
	public Object clone() {
		return new SetClause((ElementSymbol)symbol.clone(), (Expression)value.clone());
	}
	
	@Override
	public boolean equals(Object obj) {
    	if(this == obj) {
    		return true;
		}

    	if(!(obj instanceof SetClause)) {
    		return false;
		}

    	SetClause other = (SetClause) obj;
    	
    	return this.symbol.equals(other.symbol) && this.value.equals(other.value);
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.hashCode(symbol.hashCode(), value.hashCode());
	}

}
