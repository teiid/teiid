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

package com.metamatrix.console.ui.tree;

/**
 * Interface to be implemented by a DefaultMutableTreeNode which keeps its children in a
 * sorted order.
 */
public interface SortsChildren {
    public final static int NUMERIC_SORT = 1;
    public final static int ALPHABETIC_SORT = 2;
    public final static int NO_SORT = 3;

    void setSortType(int type);
        //One of the above
    int getSortType();
        //One of the above
    boolean sortChildren();
        //Returns true if sorting caused an order change, else false
}
