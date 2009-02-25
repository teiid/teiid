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

import java.util.List;

/**
 * Represents a join in the FROM clause.  A join combines two IFromItems together
 * in a join.
 */
public interface IJoin extends IFromItem {

	public enum JoinType {
		INNER_JOIN,
		CROSS_JOIN,
		LEFT_OUTER_JOIN,
		RIGHT_OUTER_JOIN,
		FULL_OUTER_JOIN
	}
	
    /**
     * Get the left IFromItem
     * @return From item
     */
    IFromItem getLeftItem();
    
    /**
     * Get the right IFromItem
     * @return From item
     */
    IFromItem getRightItem();
    
    /**
     * Get join type 
     * @return Join type
     * @see JoinType#INNER_JOIN
     * @see JoinType#CROSS_JOIN
     * @see JoinType#LEFT_OUTER_JOIN
     * @see JoinType#RIGHT_OUTER_JOIN
     * @see JoinType#FULL_OUTER_JOIN
     */
    JoinType getJoinType();
    
    /**
     * Return List of CompareCriteria specifying join criteria.
     * @return List of CompareCriteria
     */
    List<ICriteria> getCriteria();
    
    /**
     * Set the left IFromItem
     * @param item From item
     */
    void setLeftItem(IFromItem item);
    
    /**
     * Set the right IFromItem
     * @param item From item
     */
    void setRightItem(IFromItem item);
    
    /**
     * Set join type 
     * @param type Join type
     * @see JoinType#INNER_JOIN
     * @see JoinType#CROSS_JOIN
     * @see JoinType#LEFT_OUTER_JOIN
     * @see JoinType#RIGHT_OUTER_JOIN
     * @see JoinType#FULL_OUTER_JOIN
     */
    void setJoinType(JoinType type);
    
}
