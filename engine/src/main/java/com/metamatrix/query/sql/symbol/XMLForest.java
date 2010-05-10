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

package com.metamatrix.query.sql.symbol;

import java.util.List;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

public class XMLForest implements Expression {

	private static final long serialVersionUID = -3348922701950966494L;
	private List<SingleElementSymbol> args;
	private XMLNamespaces namespaces;
	
	public XMLForest(List<SingleElementSymbol> args) {
		this.args = args;
	}
	
	public XMLNamespaces getNamespaces() {
		return namespaces;
	}
	
	public void setNamespaces(XMLNamespaces namespaces) {
		this.namespaces = namespaces;
	}
	
	public List<SingleElementSymbol> getArgs() {
		return args;
	}

	@Override
	public Class<?> getType() {
		return DataTypeManager.DefaultDataClasses.XML;
	}

	@Override
	public boolean isResolved() {
		for (SingleElementSymbol arg : args) {
			if (!arg.isResolved()) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
		
	@Override
	public XMLForest clone() {
		XMLForest clone = new XMLForest(LanguageObject.Util.deepClone(args, SingleElementSymbol.class));
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
		return args.equals(other.args);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
}
