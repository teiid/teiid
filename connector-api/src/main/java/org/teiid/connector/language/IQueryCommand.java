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

package org.teiid.connector.language;

public interface IQueryCommand extends ICommand, IInsertValueSource {
    
    /**
     * Set ORDER BY clause, may be null.
     * @param orderBy An ORDER BY object
     */
    void setOrderBy(IOrderBy orderBy);    
    /**
     * Set LIMIT clause, may be null.
     * @param limit A LIMIT object
     */
    void setLimit(ILimit limit);    
    
    IQuery getProjectedQuery();
    
    /**
     * Get ORDER BY clause, may be null.
     * @return An ORDER BY object
     */
    IOrderBy getOrderBy();
    
    /**
     * Get LIMIT clause, may be null.
     * @return A LIMIT object
     */
    ILimit getLimit();
    
    /**
     * Get the column names of the output columns for this query 
     * @return a String[] containing the column names
     * @since 4.3
     */
    String[] getColumnNames();
    
    /**
     * Get the column types of the output columns for this query 
     * @return a Class[] containing the column names
     * @since 4.3
     */
    Class[] getColumnTypes();
    
}
