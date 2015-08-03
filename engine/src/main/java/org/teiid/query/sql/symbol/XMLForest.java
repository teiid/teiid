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

public class XMLForest implements Expression {

	private List<DerivedColumn> args;
	private XMLNamespaces namespaces;
	
	public XMLForest(List<DerivedColumn> args) {
		this.args = args;
	}
	
	public XMLNamespaces getNamespaces() {
		return namespaces;
	}
	
	public void setNamespaces(XMLNamespaces namespaces) {
		this.namespaces = namespaces;
	}
	
	public List<DerivedColumn> getArgs() {
		return args;
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
	public XMLForest clone() {
		XMLForest clone = new XMLForest(LanguageObject.Util.deepClone(args, DerivedColumn.class));
		if (namespaces != null) {
			clone.namespaces = namespaces.clone();
		}
		return clone;
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.hashCode(args.hashCode());
	}
	
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof XMLForest)) {
			return false;
		}
		XMLForest other = (XMLForest)obj;
		return args.equals(other.args) && EquivalenceUtil.areEqual(namespaces, other.namespaces);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
}
