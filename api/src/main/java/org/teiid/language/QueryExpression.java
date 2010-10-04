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

public abstract class QueryExpression extends BaseLanguageObject implements Command, InsertValueSource {

    private OrderBy orderBy;
    private Limit limit;
    private With with;

    public abstract Select getProjectedQuery();
    
    /**
     * Get ORDER BY clause, may be null.
     * @return An ORDER BY object
     */
    public OrderBy getOrderBy() {
        return orderBy;
    }

    /**
     * Get LIMIT clause, may be null.
     * @return A LIMIT object
     */
    public Limit getLimit() {
        return limit;
    }

    /**
     * Get the derived column names.  Note this only reports alias names.
     * Any other names may not be consistent throughout the translation process.
     * @return a String[] containing the column names
     * @since 4.3
     */
    public String[] getColumnNames() {
        List<DerivedColumn> selectSymbols = getProjectedQuery().getDerivedColumns();
        String[] columnNames = new String[selectSymbols.size()];
        int symbolIndex = 0;
        for (DerivedColumn column : selectSymbols) {
        	columnNames[symbolIndex++] = column.getAlias();
		}
        return columnNames;
    }

    /**
     * Get the column types of the output columns for this query 
     * @return a Class[] containing the column names
     * @since 4.3
     */
    public Class<?>[] getColumnTypes() {
        List<DerivedColumn> selectSymbols = getProjectedQuery().getDerivedColumns();
        Class<?>[] columnTypes = new Class[selectSymbols.size()];
        int symbolIndex = 0;
        for (DerivedColumn column : selectSymbols) {
        	columnTypes[symbolIndex++] = column.getExpression().getType();
		}
        return columnTypes;
    }
    
    /**
     * Set ORDER BY clause, may be null.
     * @param orderBy An ORDER BY object
     */
    public void setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }
    
    /**
     * Set LIMIT clause, may be null.
     * @param limit A LIMIT object
     */
    public void setLimit(Limit limit) {
        this.limit = limit;
    }
    
    public With getWith() {
		return with;
	}
    
    public void setWith(With with) {
		this.with = with;
	}
}
