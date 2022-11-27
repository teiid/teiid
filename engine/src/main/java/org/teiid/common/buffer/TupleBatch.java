/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    public static final byte NOT_TERMINATED = 0;
    public static final byte TERMINATED = 1;
    public static final byte ITERATION_TERMINATED = 2;

    private long rowOffset;
    protected List<List<?>> tuples;

    // Optional state
    private byte terminationFlag = NOT_TERMINATED;

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
    public TupleBatch(long beginRow, List<?>[] tuples) {
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
    public TupleBatch(long beginRow, List<? extends List<?>> listOfTupleLists) {
        this.rowOffset = beginRow;
        this.tuples = new ArrayList<List<?>>(listOfTupleLists);
    }

    /**
     * Return the number of the first row of the tuple source that is
     * contained in this batch (one-based).
     * @return the first row contained in this tuple batch
     */
    public long getBeginRow() {
        return rowOffset;
    }

    /**
     * Return number of the last row of the tuple source that is contained in
     * this batch (one-based).
     * @return the last row contained in this tuple batch
     */
    public long getEndRow() {
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
    public List<?> getTuple(long rowIndex) {
        long base = rowIndex - rowOffset;
        int intVal = (int)base;
        if (base != intVal) {
            throw new AssertionError("rowIndex overflow " + rowIndex); //$NON-NLS-1$
        }
        return tuples.get(intVal);
    }

    public List<List<?>> getTuples() {
        return tuples;
    }

    /**
     * Get all tuples
     * @return All tuples
     */
    public List<?>[] getAllTuples() {
        return tuples.toArray(new List[tuples.size()]);
    }

    /**
     * Check whether this batch is the last in a series of batches.
     * @return True if this batch is last
     */
    public boolean getTerminationFlag() {
        return this.terminationFlag == TERMINATED;
    }

    /**
     * Set whether this batch is the last in a series of batches.
     * @param terminationFlag True if last
     */
    public void setTerminationFlag(boolean terminationFlag) {
        this.terminationFlag = terminationFlag?TERMINATED:NOT_TERMINATED;
    }

    public void setTermination(byte val) {
        this.terminationFlag = val;
    }

    public byte getTermination() {
        return this.terminationFlag;
    }

    public boolean containsRow(long row) {
        return rowOffset <= row && getEndRow() >= row;
    }

    /**
     * Return a String describing this object
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

    public void setRowOffset(long rowOffset) {
        this.rowOffset = rowOffset;
    }
}

