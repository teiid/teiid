/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.FakeBufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.batch.TestBatchedUpdatePlanner;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.util.CommandContext;


/** 
 * @since 4.2
 */
public class TestBatchedUpdateNode extends TestCase {
    
    public TestBatchedUpdateNode(String name) {
        super(name);
    }
        
    private BatchedUpdateNode helpGetNode(String[] sql, QueryMetadataInterface md, ProcessorDataManager pdm) throws Exception {
        BatchedUpdateNode node = new BatchedUpdateNode(1, TestBatchedUpdatePlanner.helpGetCommands(sql, md), "myModelName"); //$NON-NLS-1$
        CommandContext context = new CommandContext();
        context.setProcessorID("myProcessorID"); //$NON-NLS-1$
        node.initialize(context, new FakeBufferManager(), pdm); 
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
    
    private void helpTestNextBatch(String[] commands, int[] expectedResults) throws Exception {
        int numExecutedCommands = 0;
        for (int i = 0; i < expectedResults.length; i++) {
            numExecutedCommands += expectedResults[i];
        }
        BatchedUpdateNode node = helpOpen(commands, new FakePDM(numExecutedCommands));
        try {
            TupleBatch batch = node.nextBatch();
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
        } catch (BlockedException e) {
            e.printStackTrace();
            fail("Should not have blocked on call to nextBatch()");//$NON-NLS-1$
        }
    }
    
    public void testOpen1() throws Exception {
        String[] sql = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "INSERT INTO pm1.g2 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)" //$NON-NLS-1$
        };
        String[] expectedCommands = {"BatchedUpdate{I,I}"}; //$NON-NLS-1$
        helpTestOpen(sql, expectedCommands);
    }
    
    public void testOpen2() throws Exception {
        String[] sql = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                        "UPDATE pm1.g1 SET e2 = 50 WHERE e1 = 'criteria'", //$NON-NLS-1$
                        "DELETE FROM pm1.g2 WHERE e2 = 50", //$NON-NLS-1$
                        "DELETE FROM pm1.g2 WHERE e2 = 100" //$NON-NLS-1$
        };
        String[] expectedCommands = {"BatchedUpdate{I,U,D,D}"}; //$NON-NLS-1$
        helpTestOpen(sql, expectedCommands);
    }
    
    public void testOpenAllCommandsExecuted() throws Exception {
        String[] sql = {"UPDATE pm1.g1 SET e2 = 50 WHERE e1 = 'criteria'", //$NON-NLS-1$
                        "DELETE FROM pm1.g2 WHERE e2 = 50", //$NON-NLS-1$
                        "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE e1 = 'myrow'" //$NON-NLS-1$
        };
        String[] expectedCommands = {"BatchedUpdate{U,D,U}"}; //$NON-NLS-1$
        helpTestOpen(sql, expectedCommands);
    }
    
    public void testOpenNoCommandsExecuted() throws Exception {
        String[] sql = {"UPDATE pm1.g1 SET e2 = 50 WHERE 1 = 0", //$NON-NLS-1$
                        "DELETE FROM pm1.g2 WHERE 1 = 0", //$NON-NLS-1$
                        "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE 1 = 0" //$NON-NLS-1$
        };
        String[] expectedCommands = {}; 
        helpTestOpen(sql, expectedCommands);
    }
    
    public void testOpenSomeCommandsExecuted() throws Exception {
        String[] sql = {"UPDATE pm1.g1 SET e2 = 50 WHERE e1 = 'criteria'", //$NON-NLS-1$
                        "DELETE FROM pm1.g2 WHERE 1 = 0", //$NON-NLS-1$
                        "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE e1 = 'myrow'" //$NON-NLS-1$
        };
        String[] expectedCommands = {"BatchedUpdate{U,U}"}; //$NON-NLS-1$
        helpTestOpen(sql, expectedCommands);
    }
    
    public void testNextBatch1() throws Exception {
        String[] commands = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                             "INSERT INTO pm1.g2 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)" //$NON-NLS-1$
        };
        int[] expectedResults = {1,1};
        helpTestNextBatch(commands, expectedResults);
    }
    
    public void testNextBatch2() throws Exception {
        String[] commands = {"INSERT INTO pm1.g1 (e1, e2, e3, e4) values ('string1', 1, {b'true'}, 1.0)", //$NON-NLS-1$
                             "UPDATE pm1.g1 SET e2 = 50 WHERE e1 = 'criteria'", //$NON-NLS-1$
                             "DELETE FROM pm1.g2 WHERE e2 = 50", //$NON-NLS-1$
                             "DELETE FROM pm1.g2 WHERE e2 = 100" //$NON-NLS-1$
        };
        int[] expectedResults = {1,1,1,1};
        helpTestNextBatch(commands, expectedResults);
    }
    
    public void testNextBatchAllcommandsExecuted() throws Exception {
        String[] commands = {"UPDATE pm1.g1 SET e2 = 50 WHERE e1 = 'criteria'", //$NON-NLS-1$
                             "DELETE FROM pm1.g2 WHERE e2 = 50", //$NON-NLS-1$
                             "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE e1 = 'myrow'" //$NON-NLS-1$
        };
        int[] expectedResults = {1,1,1};
        helpTestNextBatch(commands, expectedResults);
    }
    
    public void testNextBatchNoCommandsExecuted() throws Exception {
        String[] commands = {"UPDATE pm1.g1 SET e2 = 50 WHERE 1 = 0", //$NON-NLS-1$
                             "DELETE FROM pm1.g2 WHERE 1 = 0", //$NON-NLS-1$
                             "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE 1 = 0" //$NON-NLS-1$
        };
        int[] expectedResults = {0,0,0};
        helpTestNextBatch(commands, expectedResults);
    }
    
    public void testNextBatchSomeCommandsExecuted() throws Exception {
        String[] commands = {"UPDATE pm1.g1 SET e2 = 50 WHERE e1 = 'criteria'", //$NON-NLS-1$
                             "DELETE FROM pm1.g2 WHERE 1 = 0", //$NON-NLS-1$
                             "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE e1 = 'myrow'", //$NON-NLS-1$
                             "UPDATE pm1.g2 set e2 = 5, e3 = {b'false'}, e4 = 3.33 WHERE 1 = 0" //$NON-NLS-1$
        };
        int[] expectedResults = {1,0,1,0};
        helpTestNextBatch(commands, expectedResults);
    }
    
    private static final class FakePDM implements ProcessorDataManager {
    	private int numExecutedCommands;
        private List commands = new ArrayList();
        private FakePDM(int numExecutedCommands) {
        	this.numExecutedCommands = numExecutedCommands;
        }
        public Object lookupCodeValue(CommandContext context,String codeTableName,String returnElementName,String keyElementName,Object keyValue) throws BlockedException,MetaMatrixComponentException {return null;}
        public TupleSource registerRequest(Object processorID,Command command,String modelName,int nodeID) throws MetaMatrixComponentException {
            assertEquals("myProcessorID", processorID); //$NON-NLS-1$
            assertEquals("myModelName", modelName); //$NON-NLS-1$
            assertEquals(1, nodeID);
            commands.add(command.toString());
            return new FakeTupleSource(numExecutedCommands);
        }
    }
    private static final class FakeTupleSource implements TupleSource {
        private int currentTuple = 0;
        private int numCommands;
        private FakeTupleSource(int numCommands) {
            this.numCommands = numCommands;
        }
        public void closeSource() throws MetaMatrixComponentException {}
        public List getSchema() {return null;}
        public List nextTuple() throws MetaMatrixComponentException {
            if (currentTuple < numCommands) {
                return Arrays.asList(new Object[] {new Integer(1)});
            }
            return null;
        }
    }
    
}
