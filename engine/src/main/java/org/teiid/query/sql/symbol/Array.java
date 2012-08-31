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
import org.teiid.core.util.Assertion;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class Array implements Expression {

	private Class<?> type;
	private List<Expression> expressions;
	private boolean implicit;
	
	public Array(List<Expression> expressions) {
		this.expressions = expressions;
	}
	
	public Array(Class<?> baseType, List<Expression> expresssions) {
		setComponentType(baseType);
		this.expressions = expresssions;
	}
	
	@Override
	public Class<?> getType() {
		return type;
	}
	
	public void setType(Class<?> type) {
		if (type != null) {
			Assertion.assertTrue(type.isArray());
		}
		this.type = type;
	}

	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public Array clone() {
		Array clone = new Array(LanguageObject.Util.deepClone(getExpressions(), Expression.class));
		clone.type = type;
		clone.implicit = implicit;
		return clone;
	}
	
	public Class<?> getComponentType() {
		if (this.type != null) {
			return this.type.getComponentType();
		}
		return null;
	}
	
	public void setComponentType(Class<?> baseType) {
		if (baseType != null) {
			this.type = DataTypeManager.getArrayType(baseType);
		} else {
			this.type = null;
		}
	}
	
	public List<Expression> getExpressions() {
		return expressions;
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.expHashCode(type.hashCode(), getExpressions());
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
		return EquivalenceUtil.areEqual(type, other.type) && EquivalenceUtil.areEqual(expressions, other.expressions);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}

	public void setImplicit(boolean implicit) {
		this.implicit = implicit;
	}
	
	/**
	 * If the array has been implicitly constructed, such as with vararg parameters
	 * @return
	 */
	public boolean isImplicit() {
		return implicit;
	}
	
}
