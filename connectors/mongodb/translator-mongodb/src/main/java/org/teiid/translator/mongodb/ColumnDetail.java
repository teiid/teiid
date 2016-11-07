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
package org.teiid.translator.mongodb;

import java.util.ArrayList;

import com.mongodb.QueryBuilder;

class ColumnDetail {
	private ArrayList<String> projectedNames = new ArrayList<String>();
	String documentFieldName;
	Object expression;
	boolean partOfGroupBy;
	boolean partOfProject;

	public QueryBuilder getQueryBuilder() {
        QueryBuilder query = QueryBuilder.start(this.projectedNames.get(0));
        if (this.documentFieldName != null) {
            query = QueryBuilder.start(this.documentFieldName);
        }
        return query;
	}

	public QueryBuilder getPullQueryBuilder() {
	    if (this.documentFieldName != null) {
	        return QueryBuilder.start(this.documentFieldName.substring(this.documentFieldName.lastIndexOf('.')+1));
	    }
	    return QueryBuilder.start(this.projectedNames.get(0));
    }

	public void addProjectedName(String name) {
        this.projectedNames.add(0, name);
    }

	public String getProjectedName(){
	    return this.projectedNames.get(0);
	}

    public boolean hasProjectedName(String name) {
        for(String s: this.projectedNames) {
            if (s.equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

	@Override
	public String toString() {
	    StringBuilder sb = new StringBuilder();
	    sb.append("Projected Name             = ").append(this.projectedNames).append("\n");  //$NON-NLS-1$//$NON-NLS-2$
	    sb.append("Document Field Name  = ").append(this.documentFieldName).append("\n"); //$NON-NLS-1$//$NON-NLS-2$
	    sb.append("Expression = ").append(this.expression).append("\n"); //$NON-NLS-1$//$NON-NLS-2$
		return sb.toString();
	}
}