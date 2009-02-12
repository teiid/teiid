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

package com.metamatrix.query.processor.relational;

import java.util.List;

import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.util.ValueIteratorSource;


/** 
 */
public class DependentValueSource implements
                                 ValueIteratorSource {

    // Runtime
    private IndexedTupleSource tupleSource;
    private TupleSourceID tupleSourceID;
    
    public DependentValueSource() {
        super();
    }

    public void setTupleSource(IndexedTupleSource tupleSource, TupleSourceID tupleSourceID) {
        this.tupleSource = tupleSource;
        this.tupleSourceID = tupleSourceID;
    }
    
    public TupleSourceID getTupleSourceID() {
        return tupleSourceID;
    }
    
    public IndexedTupleSource getTupleSource() {
        return tupleSource;
    }
    
    public boolean isReady() {
        return this.tupleSource != null;
    }
    
    /** 
     * @see com.metamatrix.query.sql.util.ValueIteratorSource#getValueIterator(com.metamatrix.query.sql.symbol.Expression)
     */
    public ValueIterator getValueIterator(Expression valueExpression) {
        TupleSourceValueIterator iter = null;
        if(this.tupleSource != null) {
            List schema = tupleSource.getSchema();
            int columnIndex = schema.indexOf(valueExpression);
            iter = new TupleSourceValueIterator(this.tupleSource, columnIndex);
        }
        return iter;
    }
           
    public void reset() {
        // Reset runtime state
        this.tupleSource = null;
    }

}
