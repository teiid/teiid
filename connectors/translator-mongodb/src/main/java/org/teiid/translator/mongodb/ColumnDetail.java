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

import com.mongodb.QueryBuilder;

class ColumnDetail {
    String documentFieldName;
	String projectedName;
	String documentQueryFieldName;
	String targetDocumentFieldName;
	String tableName;
	String columnName;
	String targetDocumentName;
	Object expression;
	boolean partOfGroupBy;
	boolean partOfProject;

	public QueryBuilder getQueryBuilder() {
        QueryBuilder query = QueryBuilder.start(this.projectedName);
        if (this.documentQueryFieldName != null) {
            query = QueryBuilder.start(this.documentQueryFieldName); 
        }
        return query; 
	}

	public QueryBuilder getPullQueryBuilder() {
	    QueryBuilder query = QueryBuilder.start(this.projectedName);
        if (this.targetDocumentFieldName != null) {
            query = QueryBuilder.start(this.targetDocumentFieldName); 
        }
        return query;
    }
	
	@Override
	public String toString() {
	    StringBuilder sb = new StringBuilder();
	    sb.append("Column Name                = ").append(this.columnName).append("\n");  //$NON-NLS-1$//$NON-NLS-2$
	    sb.append("Document                   = ").append(this.tableName).append("\n");  //$NON-NLS-1$//$NON-NLS-2$
	    sb.append("Target Document            = ").append(this.targetDocumentName).append("\n");  //$NON-NLS-1$//$NON-NLS-2$
	    sb.append("Projected Name             = ").append(this.projectedName).append("\n");  //$NON-NLS-1$//$NON-NLS-2$
	    sb.append("Document Query Field Name  = ").append(this.documentQueryFieldName).append("\n"); //$NON-NLS-1$//$NON-NLS-2$
	    sb.append("Document Field Name        = ").append(this.documentFieldName).append("\n"); //$NON-NLS-1$//$NON-NLS-2$
	    sb.append("Target Document Field Name = ").append(this.targetDocumentFieldName).append("\n"); //$NON-NLS-1$//$NON-NLS-2$
	    sb.append("Expression = ").append(this.expression).append("\n"); //$NON-NLS-1$//$NON-NLS-2$
		return sb.toString();
	}
}