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

import java.util.ArrayList;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;

/**
 * Represents the ArrayTable table function.
 */
public class ArrayTable extends TableFunctionReference {
	
    private Expression arrayValue;
    private List<ProjectedColumn> columns = new ArrayList<ProjectedColumn>();
    
    public List<ProjectedColumn> getColumns() {
		return columns;
	}
    
    public void setColumns(List<ProjectedColumn> columns) {
		this.columns = columns;
	}
    
    public Expression getArrayValue() {
		return arrayValue;
	}
    
    public void setArrayValue(Expression arrayValue) {
		this.arrayValue = arrayValue;
	}

	@Override
	public void acceptVisitor(LanguageVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	protected ArrayTable cloneDirect() {
		ArrayTable clone = new ArrayTable();
		this.copy(clone);
		clone.setArrayValue((Expression)this.arrayValue.clone());
		for (ProjectedColumn column : columns) {
			clone.getColumns().add(column.copyTo(new ProjectedColumn()));
		}
		return clone;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!super.equals(obj) || !(obj instanceof ArrayTable)) {
			return false;
		}
		ArrayTable other = (ArrayTable)obj;
		return this.columns.equals(other.columns) 
			&& EquivalenceUtil.areEqual(arrayValue, other.arrayValue);
	}
	
}
