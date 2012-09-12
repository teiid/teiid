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
package org.teiid.translator.object.search;

import org.teiid.metadata.Column;
import org.teiid.metadata.Table;

public class SearchCriterion {

	public enum Operator {
		AND, // used to group conditional criterial
		EQUALS, IN, ALL
		// no criteria, select all objects

	}

	private SearchCriterion addCondition;
	private boolean isAnd = false;

	private Operator operator;
	private String operatorString;
	private Column column;
	private Object value;
	private String runtimeType;
	private boolean isRootTableInSelect = false;

	public SearchCriterion() {
		this.operator = Operator.ALL;
	}

	public SearchCriterion(Column column, Object value, String operaterString,
			String type) {
		this.column = column;
		this.value = value;
		this.operatorString = operaterString;
		this.runtimeType = type;
		this.operator = Operator.EQUALS;

	}

	public SearchCriterion(Column column, Object value, String operaterString,
			Operator operator, String type) {
		this(column, value, operaterString, type);
		this.operator = operator;

	}

	public Column getColumn() {
		return column;
	}

	public String getTableName() {
		Object p = column.getParent();
		if (p instanceof Table) {
			Table t = (Table) p;
			return t.getName();
		} else {
			// don't this would happen, but just in case at the moment
			assert (p.getClass().getName() != null);
		}

		return null;
	}

	public String getField() {
		return getNameInSourceFromColumn(this.column);
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public Operator getOperator() {
		return operator;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

	public String getOperatorString() {
		return this.operatorString;
	}

	public void setOperatorString(String operatorString) {
		this.operatorString = operatorString;

	}

	public String getRuntimeType() {
		return this.runtimeType;
	}

	public void setRuntimeType(String type) {
		this.runtimeType = type;
	}

	public int getCriteriaCount() {
		return 1 + (this.addCondition != null ? this.addCondition
				.getCriteriaCount() : 0);
	}

	public void addAndCriterion(SearchCriterion condition) {
		this.addCondition = condition;
		this.isAnd = true;
	}

	public void addOrCriterion(SearchCriterion condition) {
		this.addCondition = condition;
		this.isAnd = false;
	}

	public SearchCriterion getCriterion() {
		return this.addCondition;
	}

	public boolean isAndCondition() {
		return this.isAnd;
	}

	public boolean isRootTableInSelect() {
		return isRootTableInSelect;
	}

	public void setRootTableInSelect(boolean isRootTableInSelect) {
		this.isRootTableInSelect = isRootTableInSelect;
	}

	private String getNameInSourceFromColumn(Column c) {
		String name = c.getNameInSource();
		if (name == null || name.equals("")) { //$NON-NLS-1$
			return c.getName();
		}
		return name;
	}

}
