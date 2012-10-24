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

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class XMLSerialize implements Expression {

	private Boolean document;
	private Boolean declaration;
	private Expression expression;
	private String typeString;
	private Class<?> type;
	private String version;
	private String encoding;
	
	@Override
	public Class<?> getType() {
		if (type == null) {
			if (typeString == null) {
				type = DataTypeManager.DefaultDataClasses.CLOB;
			} else {
				type = DataTypeManager.getDataTypeClass(typeString);
			}
		}
		return type;
	}
	
	public String getEncoding() {
		return encoding;
	}
	
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	
	public String getVersion() {
		return version;
	}
	
	public void setVersion(String version) {
		this.version = version;
	}
	
	public Boolean getDeclaration() {
		return declaration;
	}
	
	public void setDeclaration(Boolean declaration) {
		this.declaration = declaration;
	}
	
	public Expression getExpression() {
		return expression;
	}
	
	public Boolean getDocument() {
		return document;
	}
	
	public void setDocument(Boolean document) {
		this.document = document;
	}
	
	public void setExpression(Expression expression) {
		this.expression = expression;
	}
	
	public void setTypeString(String typeString) {
		this.typeString = typeString;
	}
	
	public String getTypeString() {
		return typeString;
	}
	
	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
		
	@Override
	public XMLSerialize clone() {
		XMLSerialize clone = new XMLSerialize();
		clone.document = this.document;
		clone.expression = (Expression)this.expression.clone();
		clone.typeString = this.typeString;
		clone.type = this.type;
		clone.declaration = this.declaration;
		clone.version = this.version;
		clone.encoding = this.encoding;
		return clone;
	}
	
	public boolean isDocument() {
		return document != null && document;
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.hashCode(expression.hashCode(), getType());
	}
	
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof XMLSerialize)) {
			return false;
		}
		XMLSerialize other = (XMLSerialize)obj;
		return EquivalenceUtil.areEqual(this.document, other.document)
			&& this.expression.equals(other.expression)
			&& this.getType() == other.getType()
			&& EquivalenceUtil.areEqual(this.declaration, other.declaration)
			&& EquivalenceUtil.areEqual(this.version, other.version)
			&& EquivalenceUtil.areEqual(this.encoding, other.encoding);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
}
