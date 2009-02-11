/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.language;

import java.util.List;

/**
 * Represents a join in the FROM clause.  A join combines two IFromItems together
 * in a join.
 */
public interface IJoin extends IFromItem {

    public static final int INNER_JOIN = 0;
    public static final int CROSS_JOIN = 1;
    public static final int LEFT_OUTER_JOIN = 2;
    public static final int RIGHT_OUTER_JOIN = 3;
    public static final int FULL_OUTER_JOIN = 4;

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
     * @see #INNER_JOIN
     * @see #CROSS_JOIN
     * @see #LEFT_OUTER_JOIN
     * @see #RIGHT_OUTER_JOIN
     * @see #FULL_OUTER_JOIN
     */
    int getJoinType();
    
    /**
     * Return List of CompareCriteria specifying join criteria.
     * @return List of CompareCriteria
     */
    List getCriteria();
    
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
     * @see #INNER_JOIN
     * @see #CROSS_JOIN
     * @see #LEFT_OUTER_JOIN
     * @see #RIGHT_OUTER_JOIN
     * @see #FULL_OUTER_JOIN
     */
    void setJoinType(int type);
    
    /**
     * Set List of CompareCriteria specifying join criteria.
     * @param criteria List of CompareCriteria
     */
    void setCriteria(List criteria);    
}
