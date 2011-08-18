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
 
 package org.teiid.language;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.language.visitor.LanguageObjectVisitor;

public class WindowFunction extends BaseLanguageObject implements Expression {
	
	private AggregateFunction function;
	private WindowSpecification windowSpecification;
	
	public WindowFunction() {
		
	}
	
	public AggregateFunction getFunction() {
		return function;
	}
	
	public void setFunction(AggregateFunction expression) {
		this.function = expression;
	}
	
	public WindowSpecification getWindowSpecification() {
		return windowSpecification;
	}
	
	public void setWindowSpecification(WindowSpecification windowSpecification) {
		this.windowSpecification = windowSpecification;
	}

	@Override
	public Class<?> getType() {
		return function.getType();
	}

	@Override
	public void acceptVisitor(LanguageObjectVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.hashCode(function.hashCode(), windowSpecification);
	}
	
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof WindowFunction)) {
			return false;
		}
		WindowFunction other = (WindowFunction)obj;
		return EquivalenceUtil.areEqual(this.function, other.function) &&
		EquivalenceUtil.areEqual(this.windowSpecification, other.windowSpecification);
	}
	
}
