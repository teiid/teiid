/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.sql.lang;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

public class SetClauseList implements LanguageObject {
	
	private static final long serialVersionUID = 8174681510498719451L;
	
	private List<SetClause> setClauses;
	
	public SetClauseList() {
		this.setClauses = new ArrayList<SetClause>();
	}
	
	public SetClauseList(List<SetClause> setClauses) {
		this.setClauses = setClauses;
	}
	
	public void addClause(ElementSymbol symbol, Expression expression) {
		this.setClauses.add(new SetClause(symbol, expression));
	}
	
	public void addClause(SetClause clause) {
		this.setClauses.add(clause);
	}

	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
	@Override
	public Object clone() {
		SetClauseList copy = new SetClauseList();
		for (SetClause clause : this.setClauses) {
			copy.addClause((SetClause)clause.clone());
		}
		return copy;
	}
	
	/**
	 * @return a non-updateable map representation
	 */
	public LinkedHashMap<ElementSymbol, Expression> getClauseMap() {
		LinkedHashMap<ElementSymbol, Expression> result = new LinkedHashMap<ElementSymbol, Expression>();
		for (SetClause clause : this.setClauses) {
			result.put(clause.getSymbol(), clause.getValue());
		}
		return result;
	}
	
	public List<SetClause> getClauses() {
		return this.setClauses;
	}
	
	public boolean isEmpty() {
		return this.setClauses.isEmpty();
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this == obj) {
    		return true;
		}

    	if(!(obj instanceof SetClauseList)) {
    		return false;
		}

    	SetClauseList other = (SetClauseList) obj;
    	
    	return this.setClauses.equals(other.setClauses);
	}
	
	@Override
	public int hashCode() {
		return setClauses.hashCode();
	}
	
}
