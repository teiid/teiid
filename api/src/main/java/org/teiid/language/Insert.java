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

import java.util.Iterator;
import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;


public class Insert extends BaseLanguageObject implements BatchedCommand {

    private NamedTable table;
    private List<ColumnReference> columns;
    private InsertValueSource valueSource;
    private Iterator<? extends List<?>> parameterValues;

    public Insert(NamedTable group, List<ColumnReference> elements, InsertValueSource valueSource) {
        this.table = group;
        this.columns = elements;
        this.valueSource = valueSource;
    }

    public NamedTable getTable() {
        return this.table;
    }

    public List<ColumnReference> getColumns() {
        return this.columns;
    }

    public InsertValueSource getValueSource() {
        return this.valueSource;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setTable(NamedTable group) {
        this.table = group;
    }

    public void setColumns(List<ColumnReference> elements) {
        this.columns = elements;
    }

    public void setValueSource(InsertValueSource values) {
    	this.valueSource = values;
    }

    @Override
	public Iterator<? extends List<?>> getParameterValues() {
    	return this.parameterValues;
    }

    public void setParameterValues(Iterator<? extends List<?>> parameterValues) {
		this.parameterValues = parameterValues;
	}

}
