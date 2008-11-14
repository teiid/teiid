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

package com.metamatrix.data.basic;

import java.io.Serializable;
import java.util.*;

import com.metamatrix.data.api.Batch;

/**
 * Provides a basic implementation of the {@link com.metamatrix.data.api.Batch} interface.  
 */
public class BasicBatch implements Batch, Serializable {
    private List rows;
    private boolean isLast;

    /**
     * Construct a new empty batch.
     */
    public BasicBatch(){
        rows = new ArrayList();
    }

    /**
     * Construct a batch with the specified List of rows.
     * @param rows List of List where each internal list contains data values representing a row
     */    
    public BasicBatch(List rows){
        this.rows = rows;
    }

    /**
     * Add a row of data values
     * @param row Row of data values
     */    
    public void addRow(List row) {
        rows.add(row);
    }

    /**
     * Get count of number of rows in this batch
     * @return Number of rows in the batch
     */    
    public int getRowCount() {
        return rows.size();
    }

    /**
     * Set the flag on this batch such that the batch is marked as the last batch.
     */    
    public void setLast() {
        this.isLast = true;
    }

    /**
     * Determine whether this batch is the last batch in the execution.
     * @return True if this batch is the last
     */    
    public boolean isLast() {
        return isLast;
    }

    /**
     * Return all the rows in this batch as an array of List where each List is a row
     * of data values.
     * @return All rows in the batch
     */
    public List[] getResults() {

        return (List[]) rows.toArray(new List[0]);
    }
}
