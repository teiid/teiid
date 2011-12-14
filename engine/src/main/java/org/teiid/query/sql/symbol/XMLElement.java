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


/**
 * Represents XMLATTRIBUTES/XMLFOREST name value pairs
 */
public class XMLElement implements Expression {

	private static final long serialVersionUID = -3348922701950966494L;
	private String name;
	private XMLNamespaces namespaces;
	private XMLAttributes attributes;
	private List<Expression> content;
	
	public XMLElement(String name, List<Expression> content) {
		this.name = name;
		this.content = content;
	}
	
	public XMLAttributes getAttributes() {
		return attributes;
	}
	
	public XMLNamespaces getNamespaces() {
		return namespaces;
	}
	
	public void setAttributes(XMLAttributes attributes) {
		this.attributes = attributes;
	}
	
	public void setNamespaces(XMLNamespaces namespaces) {
		this.namespaces = namespaces;
	}
	
	public List<Expression> getContent() {
		return content;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setContent(List<Expression> args) {
		this.content = args;
	}

	@Override
	public Class<?> getType() {
		return DataTypeManager.DefaultDataClasses.XML;
	}

	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
		
	@Override
	public XMLElement clone() {
		XMLElement clone = new XMLElement(name, LanguageObject.Util.deepClone(content, Expression.class));
		if (namespaces != null) {
			clone.setNamespaces(namespaces.clone());
		}
		if (attributes != null) {
			clone.setAttributes(attributes.clone());
		}
		return clone;
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.hashCode(name.toUpperCase().hashCode(), content.hashCode());
	}
	
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof XMLElement)) {
			return false;
		}
		XMLElement other = (XMLElement)obj;
		return name.equalsIgnoreCase(other.name) && content.equals(other.content) && EquivalenceUtil.areEqual(this.namespaces, other.namespaces);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
}
