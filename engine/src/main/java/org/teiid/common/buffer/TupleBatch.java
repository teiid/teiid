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

package org.teiid.common.buffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Represents a set of indexed tuples.  The {@link #getBeginRow beginning row}
 * is the first row contained in this batch; if it equals "1" then it is the
 * first row of the tuple source, otherwise this is a batch of intermediate
 * tuples.  The {@link #getEndRow ending row} is the last row contained in 
 * this tuple batch; it is equal to the beginning row plus the 
 * {@link #getRowCount number of rows} contained in this batch, minus one.
 */
public class TupleBatch {
	
	private static final long serialVersionUID = 6304443387337336957L;
	
	private int rowOffset;    
    protected List<List<?>> tuples;
    
    // Optional state
    private boolean terminationFlag = false;
    
    /** Required to honor Externalizable contract */
    public TupleBatch() {
    }
    
    /**
     * Constructor
     * @param beginRow indicates the row of the tuple source which is the
     * first row contained in this batch
     * @param tuples array of List objects, each of which is
     * a single tuple
     */
    public TupleBatch(int beginRow, List<?>[] tuples) {
        this.rowOffset = beginRow;
        this.tuples = Arrays.asList(tuples);
    }

    /**
     * Constructor
     * @param beginRow indicates the row of the tuple source which is the
     * first row contained in this batch
     * @param listOfTupleLists List containing List objects, each of which is
     * a single tuple
     */
    public TupleBatch(int beginRow, List listOfTupleLists) {
        this.rowOffset = beginRow;
        this.tuples = new ArrayList(listOfTupleLists);
    }

    /**
     * Return the number of the first row of the tuple source that is
     * contained in this batch (one-based).
     * @return the first row contained in this tuple batch
     */
    public int getBeginRow() {
        return rowOffset;
    }
    
    /**
     * Return number of the last row of the tuple source that is contained in 
     * this batch (one-based).
     * @return the last row contained in this tuple batch
     */
    public int getEndRow() {
        return rowOffset + tuples.size() - 1;
    }
    
    /**
     * Return the number of rows contained in this tuple batch
     * @return the number of rows contained in this tuple batch
     */
    public int getRowCount() {
        return tuples.size();
    }
        
    /**
     * Return the tuple at the given index (one-based).
     * @return the tuple at the given index
     */
    public List getTuple(int rowIndex) {
        return tuples.get(rowIndex-rowOffset);
    }
    
    public List<List<?>> getTuples() {
		return tuples;
	}
    
    /**
     * Get all tuples 
     * @return All tuples
     */
    public List[] getAllTuples() { 
    	return tuples.toArray(new List[tuples.size()]);    
    }

    /**
     * Check whether this batch is the last in a series of batches.
     * @return True if this batch is last
     */
    public boolean getTerminationFlag() {
        return this.terminationFlag;    
    }
    
    /**
     * Set whether this batch is the last in a series of batches.
     * @param terminationFlag True if last
     */
    public void setTerminationFlag(boolean terminationFlag) {
        this.terminationFlag = terminationFlag;    
    }
    
    public boolean containsRow(int row) {
    	return rowOffset <= row && getEndRow() >= row;
    }
    
    /**
     * Return a String describing this object
     * @param String representation of this TupleBatch
     */
    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("TupleBatch; beginning row="); //$NON-NLS-1$
        s.append(rowOffset);
        s.append(", number of rows="); //$NON-NLS-1$
        s.append(tuples.size());
        s.append(", lastBatch="); //$NON-NLS-1$
        s.append(this.terminationFlag);
        return s.toString();
    }

    public void setRowOffset(int rowOffset) {
		this.rowOffset = rowOffset;
	}
}

