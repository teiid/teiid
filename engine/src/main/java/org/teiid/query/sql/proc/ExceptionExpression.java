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
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class ExceptionExpression implements Expression, LanguageObject {
	
	private Expression message;
	private Expression sqlState;
	private Expression errorCode;
	private Expression parent;
	
	@Override
	public Class<?> getType() {
		return DataTypeManager.DefaultDataClasses.OBJECT;
	}
	
	public ExceptionExpression() {
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof ExceptionExpression)) {
			return false;
		}
		ExceptionExpression other = (ExceptionExpression)obj;
		return EquivalenceUtil.areEqual(message, other.message) 
		&& EquivalenceUtil.areEqual(sqlState, other.sqlState)
		&& EquivalenceUtil.areEqual(errorCode, other.errorCode)
		&& EquivalenceUtil.areEqual(parent, other.parent);
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.hashCode(0, message, sqlState, errorCode);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
	@Override
	public ExceptionExpression clone() {
		ExceptionExpression clone = new ExceptionExpression();
		if (this.message != null) {
			clone.message = (Expression) this.message.clone();
		}
		if (this.sqlState != null) {
			clone.sqlState = (Expression) this.sqlState.clone();
		}
		if (this.errorCode != null) {
			clone.errorCode = (Expression) this.errorCode.clone();
		}
		if (this.parent != null) {
			clone.parent = (Expression) this.parent.clone();
		}
		return clone;
	}
	
	public Expression getErrorCode() {
		return errorCode;
	}
	
	public void setErrorCode(Expression errCode) {
		this.errorCode = errCode;
	}
	
	public Expression getSqlState() {
		return sqlState;
	}
	
	public void setSqlState(Expression sqlState) {
		this.sqlState = sqlState;
	}
	
	public Expression getMessage() {
		return message;
	}
	
	public void setMessage(Expression message) {
		this.message = message;
	}
	
	public Expression getParent() {
		return parent;
	}
	
	public void setParent(Expression parent) {
		this.parent = parent;
	}
	
	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}

	public String getDefaultSQLState() {
		return "50001";
	}

}
