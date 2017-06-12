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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionService;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

/** 
 * @since 4.2
 */
public class TestBatchedUpdatePlan {
    
    private void helpTestNextBatch(int[] commandsPerPlan) throws Exception {
        List plans = new ArrayList(commandsPerPlan.length);
        int totalCommands = 0;
        for (int i = 0; i < commandsPerPlan.length; i++) {
            totalCommands += commandsPerPlan[i];
            plans.add(new FakeProcessorPlan(commandsPerPlan[i]));
        }
        BatchedUpdatePlan plan = new BatchedUpdatePlan(plans, totalCommands, null, false);
        plan.initialize(new CommandContext(), null, null);
        TupleBatch batch = plan.nextBatch();
        assertEquals(totalCommands, batch.getRowCount());
        for (int i = 1; i <= totalCommands; i++) {
            assertEquals(new Integer(1), batch.getTuple(i).get(0));
        }
    }
    
    @Test public void testOpen() throws Exception {
        FakeProcessorPlan[] plans = new FakeProcessorPlan[4];
        for (int i = 0; i < plans.length; i++) {
            plans[i] = new FakeProcessorPlan(1);
        }
        BatchedUpdatePlan plan = new BatchedUpdatePlan(Arrays.asList(plans), plans.length, null, false);
        plan.initialize(new CommandContext(), null, null);
        plan.open();
        // First plan may or may not be opened, but all subsequent plans should not be opened.
        for (int i = 1; i < plans.length; i++) {
            assertFalse(plans[i].isOpened());
        }
    }
    
    @Test public void testMultipleBatches() throws Exception {
        FakeProcessorPlan[] plans = new FakeProcessorPlan[4];
        for (int i = 0; i < plans.length; i++) {
            TupleBatch last = new TupleBatch(2, Arrays.asList(Arrays.asList(1)));
            last.setTerminationFlag(true);
			plans[i] = new FakeProcessorPlan(Arrays.asList(Command.getUpdateCommandSymbol()), Arrays.asList(new TupleBatch(1, Arrays.asList(Arrays.asList(1))), BlockedException.INSTANCE, last));
        }
        BatchedUpdatePlan plan = new BatchedUpdatePlan(Arrays.asList(plans), plans.length*2, null, false);
        plan.initialize(new CommandContext(), null, null);
        plan.open();
        // First plan may or may not be opened, but all subsequent plans should not be opened.
        for (int i = 1; i < plans.length; i++) {
            assertFalse(plans[i].isOpened());
        }
        for (int i = 0; i < 4; i++) {
        	try {
        		plan.nextBatch();
        		fail();
        	} catch (BlockedException e) {
        	}
        }
        TupleBatch batch = plan.nextBatch();
        assertEquals(8, batch.getRowCount());
        assertTrue(batch.getTerminationFlag());
    }

    @Test public void testNextBatch1() throws Exception {
        helpTestNextBatch(new int[] {1, 5, 2, 1, 10, 1, 1});
    }
    
    @Test public void testNextBatch2() throws Exception {
        helpTestNextBatch(new int[] {5, 4, 10, 7, 22, 9, 12, 8, 11});
    }
    
    @Test public void testNextBatch3() throws Exception {
        helpTestNextBatch(new int[] {1, 1, 1, 1});
    }
    
    @Test public void testRequiresTransaction() throws Exception {
        ProcessorPlan[] plans = new ProcessorPlan[2];
        plans[0] = new FakeProcessorPlan(1);
        plans[1] = new FakeProcessorPlan(1) {
            public Boolean requiresTransaction(boolean transactionalReads) {
                return true;
            }
        };
        
        BatchedUpdatePlan plan = new BatchedUpdatePlan(Arrays.asList(plans), plans.length, null, true);
        assertTrue(plan.requiresTransaction(false));
    }
    
    @Test public void testCommandLevelTransaction() throws Exception {
        CommandContext context = new CommandContext("pID", null, null, null, 1); //$NON-NLS-1$
        context.setMetadata(RealMetadataFactory.example1Cached());
        TransactionContext tc = new TransactionContext();
        TransactionService ts = Mockito.mock(TransactionService.class);
        context.setTransactionService(ts);
        context.setTransactionContext(tc);

        ProcessorPlan[] plans = new ProcessorPlan[4];
        for (int i = 0; i < plans.length - 2; i++) {
            plans[i] = new FakeProcessorPlan(1);
        }
        for (int i = 2; i < plans.length; i++) {
            plans[i] = new FakeProcessorPlan(1) {
                public Boolean requiresTransaction(boolean transactionalReads) {
                    return true;
                }
            };
        }
        
        BatchedUpdatePlan plan = new BatchedUpdatePlan(Arrays.asList(plans), plans.length, null, false);
        plan.requiresTransaction(false);
        plan.initialize(context, null, null);
        plan.open();
        
        TupleBatch batch = plan.nextBatch();
        assertEquals(4, batch.getRowCount());
        
        Mockito.verify(ts, Mockito.times(2)).begin(tc);
        Mockito.verify(ts, Mockito.times(2)).commit(tc);
    }
    
}
