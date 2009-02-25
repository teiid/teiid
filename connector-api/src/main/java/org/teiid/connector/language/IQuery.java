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

/**
 * Represents a SELECT query in the language objects.
 */
public interface IQuery extends IQueryCommand {

    /**
     * Get SELECT clause, should never be null.
     * @return Select clause object
     */
    ISelect getSelect();
    
    /**
     * Get FROM clause, should never be null.
     * @return From clause object
     */
    IFrom getFrom();
    
    /**
     * Get WHERE clause, may be null.
     * @return A criteria object
     */
    ICriteria getWhere();
    
    /**
     * Get GROUP BY clause, may be null.
     * @return A group by object
     */
    IGroupBy getGroupBy();
    
    /**
     * Get HAVING clause, may be null.
     * @return A criteria object
     */
    ICriteria getHaving();
    
    /**
     * Set SELECT clause, should never be null.
     * @param select Select clause object
     */
    void setSelect(ISelect select);
    
    /**
     * Set FROM clause, should never be null.
     * @param from From clause object
     */
    void setFrom(IFrom from);
    
    /**
     * Set WHERE clause, may be null.
     * @param criteria A criteria object
     */
    void setWhere(ICriteria criteria);
    
    /**
     * Set GROUP BY clause, may be null.
     * @param groupBy A group by object
     */
    void setGroupBy(IGroupBy groupBy);
    
    /**
     * Set HAVING clause, may be null.
     * @param criteria A criteria object
     */
    void setHaving(ICriteria criteria);
    
}
