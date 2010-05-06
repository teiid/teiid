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
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

/**
 * Represents XMLATTRIBUTES or XMLFOREST name value pairs
 */
public class SQLXMLFunction implements Expression {

	private static final long serialVersionUID = -3348922701950966494L;
	private List<SingleElementSymbol> args;
	private String name;
	
	public SQLXMLFunction(String name, List<SingleElementSymbol> args) {
		this.name = name;
		this.args = args;
	}
	
	public List<SingleElementSymbol> getArgs() {
		return args;
	}
	
	public String getName() {
		return name;
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
	public SQLXMLFunction clone() {
		return new SQLXMLFunction(name, LanguageObject.Util.deepClone(args, SingleElementSymbol.class));
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.hashCode(name.toUpperCase().hashCode(), args.hashCode());
	}
	
	public boolean equals(Object obj) {
		if (!(obj instanceof SQLXMLFunction)) {
			return false;
		}
		SQLXMLFunction other = (SQLXMLFunction)obj;
		return name.equalsIgnoreCase(other.name) && args.equals(other.args);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
}
