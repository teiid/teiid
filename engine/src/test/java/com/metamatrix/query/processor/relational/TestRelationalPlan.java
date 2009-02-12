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

import java.util.ArrayList;

import junit.framework.*;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.TupleBatch;

/**
 */
public class TestRelationalPlan extends TestCase {

    /**
     * Constructor for TestRelationalPlan.
     * @param arg0
     */
    public TestRelationalPlan(String arg0) {
        super(arg0);
    }
    
    public void testNoRowsFirstBatch() {
        RelationalNode node = new RelationalNode(0) {
            public TupleBatch nextBatchDirect() throws BlockedException, MetaMatrixComponentException {
                TupleBatch batch = new TupleBatch(1, new ArrayList());
                batch.setTerminationFlag(true);
                return batch;
            }    
            
            public Object clone(){
				throw new UnsupportedOperationException();
			}
        };
        
        try {        
            RelationalPlan plan = new RelationalPlan(node);
            TupleBatch batch = plan.nextBatch();
            assertTrue("Did not get terminator batch", batch.getTerminationFlag()); //$NON-NLS-1$
            
        } catch(MetaMatrixException e) {
            e.printStackTrace();
            fail("Got unexpected exception: " + e.getFullMessage());     //$NON-NLS-1$
        }
        
        
    }   
    
    public void testNoRowsLaterBatch() {
        RelationalNode node = new RelationalNode(0) {
            public TupleBatch nextBatchDirect() throws BlockedException, MetaMatrixComponentException {
                TupleBatch batch = new TupleBatch(3, new ArrayList());
                batch.setTerminationFlag(true);
                return batch;
            }  
            
            public Object clone(){
				throw new UnsupportedOperationException();
			}  
        };
        
        try {        
            RelationalPlan plan = new RelationalPlan(node);
            TupleBatch batch = plan.nextBatch();
            assertTrue("Did not get terminator batch", batch.getTerminationFlag()); //$NON-NLS-1$
            
        } catch(MetaMatrixException e) {
            e.printStackTrace();
            fail("Got unexpected exception: " + e.getFullMessage());     //$NON-NLS-1$
        }                
    }          
    
}
