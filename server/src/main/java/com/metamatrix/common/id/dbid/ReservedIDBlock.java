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

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * Used by DBIDGenerator to reserve a block of uniqueIDs used
 * to create ID objects.
 */
public class ReservedIDBlock implements Serializable {

    /**
     * Indicates that all the id's have been used up for this block
     */
    public final static long NO_ID_AVAILABLE = -1;

    private String context;

    private long first;
    private long last;
    private long next;

    // indicates the maximum number this context can have
    private long max;

    // controlls if when the max number is reached, whether the
    // numbers will wrap around and start over.
    private boolean wrappable = false;

    /**
     * Construct a new instance with the first ID and last ID in the block.
     * @param first Defines the first id in this block.
     * @param last Defines the last id in the block.
     * @throws IllegalArgumentException if first > last
     */
    public ReservedIDBlock(String context, long first, long last, long max) {

        if (first > last) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.ID_ERR_0013,
            		new Object[] {String.valueOf(first), String.valueOf(last)}));
        }

        this.context = context;

        this.first = first;
        this.next = first;
        if (last > max) {
            last = max;
        }

        this.last = last;
        this.max = max;
    }

    /**
     * Return the next ID in the block. If no id is available
     * then return NO_ID_AVAILABLE
     * @return long nextID in block
     */
    public long getNextID() {
        if (next > last) {
            return NO_ID_AVAILABLE;
        }
        next++;
        return next-1;
    }

    /**
     * Return the last ID that will be used in the block;
     * @return long lastID in block
     */
    public long getLast() {
        return last;
    }

    /**
     * Return the context for this ID block;
     * @return String context
     */

    public String getContext() {
        return context;
    }

    /**
     * Returns boolean indicating if block is all used up.
     * @return true if block is depleted.
     */
     public boolean isDepleted() {
        return (next > last);
    }

    public boolean isAtMaximum()  {
        if (isDepleted()) {
          return (last >= max ? true : false);
        }
        return false;
    }

    /**
     * Call to enable this context to reuse its numbers
     * when the maximum number is reached.
     */
    public void setIsWrappable(boolean enableWrapping) {
        wrappable = enableWrapping;
    }

    /**
     * Returns boolean indicating if the numbers can be reused
     * when the maximum number is reached.
     * @return true if block is wrappable
     */
    public boolean isWrappable() {
        return wrappable;
    }

    /**
     * Sets the maximum number allowed for this context
     * @param long nexMax is the new maximum number allowed
     */
//    public void setMax(long newMax) {
//        this.max = newMax;
 //   }

    /**
     * Return the maximum number allowed for this context
     * @return long maximum number
     */
    public long getMax() {
        return max;
    }

    /**
     * Return String representation of this instance
     * @return String representation
     */
    public String toString() {
        return "ReservedIDBlock: first = " + first + //$NON-NLS-1$
                                 " last = " + last + //$NON-NLS-1$
                                 " next = " + next + //$NON-NLS-1$
                                 " max = " + max + //$NON-NLS-1$
                                 " wrappable = " + wrappable; //$NON-NLS-1$
    }
}

