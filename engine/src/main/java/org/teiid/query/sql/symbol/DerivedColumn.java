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

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class DerivedColumn implements LanguageObject {

    private String alias;
    private Expression expression;
    private boolean propagateName = true;
    
    public DerivedColumn(String name, Expression expression) {
        this.alias = name;
        this.expression = expression;
    }
    
    public boolean isPropagateName() {
		return propagateName;
	}
    
    public void setPropagateName(boolean propagateName) {
		this.propagateName = propagateName;
	}
    
    public String getAlias() {
        return alias;
    }

    public Expression getExpression() {
        return expression;
    }

    public void setAlias(String name) {
        this.alias = name;
    }
    
    public void setExpression(Expression expression) {
        this.expression = expression;
    }
    
    @Override
    public void acceptVisitor(LanguageVisitor visitor) {
    	visitor.visit(this);
    }
    
    @Override
    public int hashCode() {
    	return HashCodeUtil.hashCode(0, alias, expression);
    }
    
    @Override
    public boolean equals(Object obj) {
    	if (obj == this) {
    		return true;
    	}
    	if (!(obj instanceof DerivedColumn)) {
    		return false;
    	}
    	DerivedColumn other = (DerivedColumn)obj;
    	return EquivalenceUtil.areEqual(alias, other.alias) && this.expression.equals(other.expression);
    }
    
    @Override
    public DerivedColumn clone() {
    	DerivedColumn clone = new DerivedColumn(alias, (Expression)this.expression.clone());
    	clone.propagateName = propagateName;
    	return clone;
    }
    
    @Override
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }

}
