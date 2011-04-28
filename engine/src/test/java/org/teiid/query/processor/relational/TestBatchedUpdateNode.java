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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestBatchedUpdatePlanner;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.util.CommandContext;



/** 
 * @since 4.2
 */
public class TestBatchedUpdateNode {
            
    private BatchedUpdateNode helpGetNode(String[] sql, QueryMetadataInterface md, ProcessorDataManager pdm) throws Exception {
    	List<Command> commands = TestBatchedUpdatePlanner.helpGetCommands(sql, md);
    	List<Boolean> shouldEvaluate = new ArrayList<Boolean>(commands.size());
    	for (Command command : commands) {
			shouldEvaluate.add(EvaluatableVisitor.needsProcessingEvaluation(command));
		}
        BatchedUpdateNode node = new BatchedUpdateNode(1, commands, Collections.EMPTY_LIST, shouldEvaluate, "myModelName"); //$NON-NLS-1$
        CommandContext context = new CommandContext();
        context.setProcessorID("myProcessorID"); //$NON-NLS-1$
        context.setMetadata(md);
        node.initialize(context, Mockito.mock(BufferManager.class), pdm); 
        return node;
    }
    
    private BatchedUpdateNode helpOpen(String[] commands, ProcessorDataManager pdm) throws Exception {
        BatchedUpdateNode node = helpGetNode(commands, FakeMetadataFactory.example1Cached(), pdm);
        node.open();
        return node;
    }
    
    private void helpTestOpen(String[] commands, String[] expectedCommands) throws Exception {
        FakePDM pdm = new FakePDM(expectedCommands.length);
        helpOpen(commands, pdm);
        assertEquals(Arrays.asList(expectedCommands), pdm.commands);
    }
    
    private FakePDM helpTestNextBatch(String[] commands, int[] expectedResults) throws Exception {
        int numExecutedCommands = 0;
        for (int i = 0; i < expectedResults.length; i++) {
            numExecutedCommands += expectedResults[i];
        }
        FakePDM fakePDM = new FakePDM(numExecutedCommands);
        BatchedUpdateNode node = helpOpen(commands, fakePDM);
        TupleBatch batch = null;
        try {
        	batch = node.nextBatch();
        } catch (BlockedException e) {
        	batch = node.nextBatch();
        }
        assertNotNull(batch);
        assertTrue(batch.getTerminationFlag());
        assertEquals(expectedResults.length, batch.getRowCount());
        for (int i = 0; i < expectedResults.length; i++) {
            List tuple = batch.getTuple(i+1);
            assertNotNull(tuple);
            Object result = tuple.get(0);
            assertNotNull(result);
            assertEquals(new Integer(expectedResults[i]), result);
        }
        return fakePDM;
    }
    
    @Test public void testOpen1() throws Exception {
        String[] sql = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "INSERT INTO pm1.g2 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)" //$NON-NLS-1$
        };
        String[] expectedCommands = {"BatchedUpdate{I,I}"}; //$NON-NLS-1$
        helpTestOpen(sql, expectedCommands);
    }
    
    @Test public void testOpen2() throws Exception {
        String[] sql = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "UPDATE pm1.g1 SET e2 = 50 WHERE e1 = 'criteria'", //$NON-NLS-1$
                        "DELETE FROM pm1.g2 WHERE e2 = 50", //$NON-NLS-1$
                        "DELETE FROM pm1.g2 WHERE e2 = 100" //$NON-NLS-1$
        };
        String[] expectedCommands = {"BatchedUpdate{I,U,D,D}"}; //$NON-NLS-1$
        helpTestOpen(sql, expectedCommands);
    }
    
    @Test public void testOpenAllCommandsExecuted() throws Exception {
        String[] sql = {"UPDATE pm1.g1 SET e2 = 50 WHERE e1 = 'criteria'", //$NON-NLS-1$
                        "DELETE FROM pm1.g2 WHERE e2 = 50", //$NON-NLS-1$
                        "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE e1 = 'myrow'" //$NON-NLS-1$
        };
        String[] expectedCommands = {"BatchedUpdate{U,D,U}"}; //$NON-NLS-1$
        helpTestOpen(sql, expectedCommands);
    }
    
    @Test public void testOpenNoCommandsExecuted() throws Exception {
        String[] sql = {"UPDATE pm1.g1 SET e2 = 50 WHERE 1 = 0", //$NON-NLS-1$
                        "DELETE FROM pm1.g2 WHERE 1 = 0", //$NON-NLS-1$
                        "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE 1 = 0" //$NON-NLS-1$
        };
        String[] expectedCommands = {}; 
        helpTestOpen(sql, expectedCommands);
    }
    
    @Test public void testOpenSomeCommandsExecuted() throws Exception {
        String[] sql = {"UPDATE pm1.g1 SET e2 = 50 WHERE e1 = 'criteria'", //$NON-NLS-1$
                        "DELETE FROM pm1.g2 WHERE 1 = 0", //$NON-NLS-1$
                        "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE e1 = 'myrow'" //$NON-NLS-1$
        };
        String[] expectedCommands = {"BatchedUpdate{U,U}"}; //$NON-NLS-1$
        helpTestOpen(sql, expectedCommands);
    }
    
    @Test public void testNextBatch1() throws Exception {
        String[] commands = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                             "INSERT INTO pm1.g2 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)" //$NON-NLS-1$
        };
        int[] expectedResults = {1,1};
        helpTestNextBatch(commands, expectedResults);
    }
    
    @Test public void testNextBatch2() throws Exception {
        String[] commands = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                             "UPDATE pm1.g1 SET e2 = 50 WHERE e1 = 'criteria'", //$NON-NLS-1$
                             "DELETE FROM pm1.g2 WHERE e2 = 50", //$NON-NLS-1$
                             "DELETE FROM pm1.g2 WHERE e2 = 100" //$NON-NLS-1$
        };
        int[] expectedResults = {1,1,1,1};
        helpTestNextBatch(commands, expectedResults);
    }
    
    @Test public void testNextBatchAllcommandsExecuted() throws Exception {
        String[] commands = {"UPDATE pm1.g1 SET e2 = 50 WHERE e1 = 'criteria'", //$NON-NLS-1$
                             "DELETE FROM pm1.g2 WHERE e2 = 50", //$NON-NLS-1$
                             "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE e1 = 'myrow'" //$NON-NLS-1$
        };
        int[] expectedResults = {1,1,1};
        helpTestNextBatch(commands, expectedResults);
    }
    
    @Test public void testNextBatchNoCommandsExecuted() throws Exception {
        String[] commands = {"UPDATE pm1.g1 SET e2 = 50 WHERE 1 = 0", //$NON-NLS-1$
                             "DELETE FROM pm1.g2 WHERE 1 = 0", //$NON-NLS-1$
                             "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE 1 = 0" //$NON-NLS-1$
        };
        int[] expectedResults = {0,0,0};
        helpTestNextBatch(commands, expectedResults);
    }
    
    @Test public void testNextBatchSomeCommandsExecuted() throws Exception {
        String[] commands = {"DELETE FROM pm1.g2 WHERE 1 = 0", //$NON-NLS-1$
        					 "UPDATE pm1.g1 SET e2 = 50 WHERE e1 = 'criteria'", //$NON-NLS-1$
                             "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE e1 = 'myrow'", //$NON-NLS-1$
                             "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE 1 = 0" //$NON-NLS-1$
        };
        int[] expectedResults = {0,1,1,0};
        helpTestNextBatch(commands, expectedResults);
    }
    
    @Test public void testNextBatchCommandNeedsEvaluated() throws Exception {
        String[] commands = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values (commandpayload(), 1, {b'true'}, 1.0)" //$NON-NLS-1$
        };
        int[] expectedResults = {1};
        FakePDM fpdm = helpTestNextBatch(commands, expectedResults);
        assertEquals("INSERT INTO pm1.g1 (e1, e2, e3, e4) VALUES (null, 1, TRUE, 1.0)", ((BatchedUpdateCommand)fpdm.actualCommands.get(0)).getUpdateCommands().get(0).toString()); //$NON-NLS-1$
    }
    
    private static final class FakePDM implements ProcessorDataManager {
    	private int numExecutedCommands;
        private List<String> commands = new ArrayList<String>();
        private List<Command> actualCommands = new ArrayList<Command>();
        private FakePDM(int numExecutedCommands) {
        	this.numExecutedCommands = numExecutedCommands;
        }
        public Object lookupCodeValue(CommandContext context,String codeTableName,String returnElementName,String keyElementName,Object keyValue) throws BlockedException,TeiidComponentException {return null;}
        public TupleSource registerRequest(CommandContext context,Command command,String modelName,String connectorBindingId, int nodeID, int limit) throws TeiidComponentException {
            assertEquals("myProcessorID", context.getProcessorID()); //$NON-NLS-1$
            assertEquals("myModelName", modelName); //$NON-NLS-1$
            assertEquals(1, nodeID);
            commands.add(command.toString());
            actualCommands.add(command);
            return new FakeTupleSource(numExecutedCommands);
        }
    }
    private static final class FakeTupleSource implements TupleSource {
        private int currentTuple = 0;
        private int numCommands;
        private boolean first = true;
        private FakeTupleSource(int numCommands) {
            this.numCommands = numCommands;
        }
        public void closeSource() {}
        public List nextTuple() throws TeiidComponentException {
            if (first) {
            	first = false;
            	throw BlockedException.INSTANCE;
            }
        	if (currentTuple++ < numCommands) {
                return Arrays.asList(new Object[] {new Integer(1)});
            }
            return null;
        }
        
    }
    
}
