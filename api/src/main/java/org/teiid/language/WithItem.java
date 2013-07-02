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

import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;

public class WithItem extends BaseLanguageObject implements SubqueryContainer {
	
	private NamedTable table;
	private List<ColumnReference> columns;
	private QueryExpression queryExpression;
	private List<? extends List<?>> dependentValues;

	public NamedTable getTable() {
		return table;
	}
	
	public void setTable(NamedTable table) {
		this.table = table;
	}
	
	public List<ColumnReference> getColumns() {
		return columns;
	}
	
	public void setColumns(List<ColumnReference> columns) {
		this.columns = columns;
	}
	
	@Override
	public QueryExpression getSubquery() {
		return queryExpression;
	}

	@Override
	public void setSubquery(QueryExpression query) {
		this.queryExpression = query;
	}

	@Override
	public void acceptVisitor(LanguageObjectVisitor visitor) {
		visitor.visit(this);
	}

	public void setDependentValues(List<? extends List<?>> tupleBufferList) {
		this.dependentValues = tupleBufferList;
	}
	
	public List<? extends List<?>> getDependentValues() {
		return dependentValues;
	}
	
}
