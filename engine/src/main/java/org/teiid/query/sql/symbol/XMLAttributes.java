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

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Represents XMLATTRIBUTES name value pairs
 */
public class XMLAttributes implements LanguageObject {

	private static final long serialVersionUID = -3348922701950966494L;
	private List<DerivedColumn> args;
	
	public XMLAttributes(List<DerivedColumn> args) {
		this.args = args;
	}
	
	public List<DerivedColumn> getArgs() {
		return args;
	}
	
	@Override
	public XMLAttributes clone() {
		XMLAttributes clone = new XMLAttributes(LanguageObject.Util.deepClone(args, DerivedColumn.class));
		return clone;
	}
	
	@Override
	public int hashCode() {
		return args.hashCode();
	}
	
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof XMLAttributes)) {
			return false;
		}
		XMLAttributes other = (XMLAttributes)obj;
		return args.equals(other.args);
	}

	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
}
