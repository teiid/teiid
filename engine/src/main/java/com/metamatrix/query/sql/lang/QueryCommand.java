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

package com.metamatrix.query.sql.lang;



/**
 * This is a common super class for the two types of query commands: Query and SetQuery.
 * This class provides some useful commonalities when the type of query command 
 * is not known.   
 */
public abstract class QueryCommand extends Command {

	/** The order in which to sort the results */
	private OrderBy orderBy;

    /** Limit on returned rows */
    private Limit limit;
        
    /**
     * Get the order by clause for the query.
     * @return order by clause
     */
    public OrderBy getOrderBy() {
        return orderBy;
    }
    
    /**
     * Set the order by clause for the query.
     * @param orderBy New order by clause
     */
    public void setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }

    public Limit getLimit() {
        return limit;
    }

    public void setLimit(Limit limit) {
        this.limit = limit;
    }
    
	public abstract Query getProjectedQuery();
}
