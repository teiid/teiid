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

package com.metamatrix.common.id.dbid;

import java.io.Serializable;

/**
 * Used by DBIDGenerator to reserve a block of uniqueIDs used
 * to create ID objects.
 */
public class ReservedIDBlock implements Serializable {

    /**
     * Indicates that all the id's have been used up for this block
     */
    public final static long NO_ID_AVAILABLE = -1;

    private long sequence;

    // indicates the maximum number this context can have
    private long max;

    /**
     * Construct a new instance with the first ID and last ID in the block.
     * @param first Defines the first id in this block.
     * @param last Defines the last id in the block.
     * @throws IllegalArgumentException if first > last
     */
    public ReservedIDBlock() {
    	
    }
    
    public void setBlockValues(long first, long max) {
    	this.sequence = first;
        this.max = max;
    }

    /**
     * Return the next ID in the block. If no id is available
     * then return NO_ID_AVAILABLE
     * @return long nextID in block
     */
    public long getNextID() {
    	if (max == 0) {
    		return NO_ID_AVAILABLE;
    	}
    	long result = sequence++;
        if (result > max) {
            return NO_ID_AVAILABLE;
        }
        return result;
    }

}

