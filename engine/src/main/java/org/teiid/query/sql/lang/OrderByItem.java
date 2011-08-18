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

package org.teiid.query.sql.lang;

import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class OrderByItem implements LanguageObject {
	
	private static final long serialVersionUID = 6937561370697819126L;
	
	private Integer expressionPosition; //set during resolving to the select clause position
	private boolean ascending = true;
	private SingleElementSymbol symbol;
	private NullOrdering nullOrdering;

	public OrderByItem(SingleElementSymbol symbol, boolean ascending) {
		this.symbol = symbol;
		this.ascending = ascending;
	}
	
	public int getExpressionPosition() {
		return expressionPosition == null?-1:expressionPosition;
	}

	public void setExpressionPosition(int expressionPosition) {
		this.expressionPosition = expressionPosition;
	}

	public boolean isAscending() {
		return ascending;
	}

	public void setAscending(boolean ascending) {
		this.ascending = ascending;
	}

	public SingleElementSymbol getSymbol() {
		return symbol;
	}

	public void setSymbol(SingleElementSymbol symbol) {
		this.symbol = symbol;
	}
	
	public NullOrdering getNullOrdering() {
		return nullOrdering;
	}
	
	public void setNullOrdering(NullOrdering nullOrdering) {
		this.nullOrdering = nullOrdering;
	}

	/**
	 * 
	 * @return true if the expression does not appear in the select clause
	 */
	public boolean isUnrelated() {
		return getExpressionPosition() == -1;
	}

	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public OrderByItem clone() {
		OrderByItem clone = new OrderByItem((SingleElementSymbol)this.symbol.clone(), ascending);
		clone.expressionPosition = this.expressionPosition;
		clone.nullOrdering = this.nullOrdering;
		return clone;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof OrderByItem)) {
			return false;
		}
		OrderByItem o = (OrderByItem)obj;
		return o.symbol.equals(symbol) && o.ascending == this.ascending && o.nullOrdering == this.nullOrdering;
	}
	
	@Override
	public int hashCode() {
		return symbol.hashCode();
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
}
