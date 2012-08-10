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

import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class Array implements Expression {

	private Class<?> baseType;
	private List<Expression> expressions;
	
	public Array(Class<?> baseType, List<Expression> expresssions) {
		this.baseType = baseType;
		this.expressions = expresssions;
	}
	
	@Override
	public Class<?> getType() {
		return DataTypeManager.DefaultDataClasses.OBJECT;
	}

	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public Array clone() {
		return new Array(baseType, LanguageObject.Util.deepClone(getExpressions(), Expression.class));
	}
	
	public Class<?> getBaseType() {
		return baseType;
	}
	
	public void setBaseType(Class<?> baseType) {
		this.baseType = baseType;
	}
	
	public List<Expression> getExpressions() {
		return expressions;
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.hashCode(0, getExpressions());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
	    if (!(obj instanceof Array)) {
	    	return false;
	    }
		Array other = (Array) obj;
		return EquivalenceUtil.areEqual(baseType, other.baseType) && EquivalenceUtil.areEqual(expressions, other.expressions);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
}
