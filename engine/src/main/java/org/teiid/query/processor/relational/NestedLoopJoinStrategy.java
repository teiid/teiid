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

package org.teiid.query.processor.relational;

import java.util.List;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.processor.relational.SourceState.ImplicitBuffer;


/**
 * Nested loop is currently implemented as a degenerate case of merge join. 
 * 
 * Only for use with Full, Left, Inner, and Cross joins
 * 
 */
public class NestedLoopJoinStrategy extends MergeJoinStrategy {

    public NestedLoopJoinStrategy() {
        super(SortOption.ALREADY_SORTED, SortOption.ALREADY_SORTED, false);
    }
    
    /** 
     * @see org.teiid.query.processor.relational.MergeJoinStrategy#clone()
     */
    @Override
    public NestedLoopJoinStrategy clone() {
        return new NestedLoopJoinStrategy();
    }
    
    /** 
     * @see org.teiid.query.processor.relational.MergeJoinStrategy#compare(java.util.List, java.util.List, int[], int[])
     */
    @Override
    protected int compare(List leftProbe,
                          List rightProbe,
                          int[] leftExpressionIndecies,
                          int[] rightExpressionIndecies) {
        return 0; // there are no expressions in nested loop joins, comparison is meaningless
    }
    
    @Override
    protected void loadRight() throws TeiidComponentException,
    		TeiidProcessingException {
    	this.rightSource.setImplicitBuffer(ImplicitBuffer.FULL);
    }
    
    /** 
     * @see org.teiid.query.processor.relational.MergeJoinStrategy#toString()
     */
    @Override
    public String toString() {
        return "NESTED LOOP JOIN"; //$NON-NLS-1$
    }
    
}