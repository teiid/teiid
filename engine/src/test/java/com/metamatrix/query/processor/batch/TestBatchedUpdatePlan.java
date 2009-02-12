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

package com.metamatrix.query.processor.batch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.query.processor.BaseProcessorPlan;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.util.CommandContext;


/** 
 * @since 4.2
 */
public class TestBatchedUpdatePlan extends TestCase {
    private int plansOpened = 0;
    private int nextBatchesCalled = 0;
    public TestBatchedUpdatePlan(String name) {
        super(name);
    }
    
    private void helpTestNextBatch(int[] commandsPerPlan) throws Exception {
        List plans = new ArrayList(commandsPerPlan.length);
        int totalCommands = 0;
        for (int i = 0; i < commandsPerPlan.length; i++) {
            totalCommands += commandsPerPlan[i];
            plans.add(new FakeProcessorPlan(commandsPerPlan[i]));
        }
        BatchedUpdatePlan plan = new BatchedUpdatePlan(plans, totalCommands);
        TupleBatch batch = plan.nextBatch();
        assertEquals(totalCommands, batch.getRowCount());
        for (int i = 1; i <= totalCommands; i++) {
            assertEquals(new Integer(1), batch.getTuple(i).get(0));
        }
    }
    
    public void testOpen() throws Exception {
        FakeProcessorPlan[] plans = new FakeProcessorPlan[4];
        for (int i = 0; i < plans.length; i++) {
            plans[i] = new FakeProcessorPlan(1);
        }
        BatchedUpdatePlan plan = new BatchedUpdatePlan(Arrays.asList(plans), plans.length);
        plan.open();
        // First plan may or may not be opened, but all subsequent plans should not be opened.
        for (int i = 1; i < plans.length; i++) {
            assertFalse(plans[i].opened);
        }
    }

    public void testNextBatch1() throws Exception {
        helpTestNextBatch(new int[] {1, 5, 2, 1, 10, 1, 1});
    }
    
    public void testNextBatch2() throws Exception {
        helpTestNextBatch(new int[] {5, 4, 10, 7, 22, 9, 12, 8, 11});
    }
    
    public void testNextBatch3() throws Exception {
        helpTestNextBatch(new int[] {1, 1, 1, 1});
    }
    
    private class FakeProcessorPlan extends BaseProcessorPlan {
        private int counts = 0;
        private boolean opened = false;
        private int updateConnectorCount = 1;
        private FakeProcessorPlan(int commands) {
            counts = commands;
        }       
        public Object clone() {return null;}
        public void close() throws MetaMatrixComponentException {}
        public void connectTupleSource(TupleSource source, int dataRequestID) {}
        public List getOutputElements() {return null;}
        public int getUpdateCount() {return updateConnectorCount;}
        public void initialize(CommandContext context, ProcessorDataManager dataMgr, BufferManager bufferMgr) {}
        public TupleBatch nextBatch() throws BlockedException, MetaMatrixComponentException {
            nextBatchesCalled++;
            assertTrue(opened);
            assertTrue(nextBatchesCalled == plansOpened);
            List[] rows = new List[counts];
            for (int i = 0; i < counts; i++) {
                rows[i] = Arrays.asList(new Object[] {new Integer(1)});
            }
            TupleBatch batch = new TupleBatch(1, rows);
            batch.setTerminationFlag(true);
            return batch;
        }
        public void open() throws MetaMatrixComponentException {
            assertFalse("ProcessorPlan.open() should not be called more than once", opened); //$NON-NLS-1$
            opened = true;
            plansOpened++;
        }
        public Map getDescriptionProperties() {return null;}
        public Collection getChildPlans() {return Collections.EMPTY_LIST;}
        
    }
}
