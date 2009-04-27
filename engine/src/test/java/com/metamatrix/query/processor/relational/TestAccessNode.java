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

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.resolver.TestResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SetClauseList;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.util.CommandContext;


/** 
 * @since 4.2
 */
public class TestAccessNode extends TestCase {

    private void helpTestOpen(Command command, String expectedCommand, boolean shouldRegisterRequest) throws Exception {
        // Setup
        AccessNode node = new AccessNode(1);
        node.setCommand(command);
        CommandContext context = new CommandContext();
        context.setProcessorID("processorID"); //$NON-NLS-1$
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        FakePDM dataManager = new FakePDM(expectedCommand);
        
        node.initialize(context, bm, dataManager);
        node.setShouldEvaluateExpressions(true);
        // Call open()
        node.open();
        
        assertEquals(shouldRegisterRequest, dataManager.registerRequestCalled);
    }
    
    public void testOpen_Defect16059() throws Exception {
    	Query query = (Query)TestResolver.helpResolve("SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5 AND ? IS NULL", FakeMetadataFactory.example1Cached(), null); //$NON-NLS-1$
        IsNullCriteria nullCrit = (IsNullCriteria)((CompoundCriteria)query.getCriteria()).getCriteria(1);
        nullCrit.setExpression(new Constant(null));
        
        helpTestOpen(query, "SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5", true); //$NON-NLS-1$
    }
    
    public void testOpen_Defect16059_2() throws Exception {
    	Query query = (Query)TestResolver.helpResolve("SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5 AND ? IS NOT NULL", FakeMetadataFactory.example1Cached(), null); //$NON-NLS-1$
        IsNullCriteria nullCrit = (IsNullCriteria)((CompoundCriteria)query.getCriteria()).getCriteria(1);
        nullCrit.setExpression(new Constant(null));
        
        helpTestOpen(query, null, false);
    }
    
    public void testExecCount()throws Exception{
        // Setup
        AccessNode node = new AccessNode(1);
    	Query query = (Query)TestResolver.helpResolve("SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5", FakeMetadataFactory.example1Cached(), null); //$NON-NLS-1$
        node.setCommand(query);
        CommandContext context = new CommandContext();
        context.setProcessorID("processorID"); //$NON-NLS-1$
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        FakePDM dataManager = new FakePDM("SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5"); //$NON-NLS-1$
        node.initialize(context, bm, dataManager);
        // Call open()
        node.open();
        assertTrue(dataManager.registerRequestCalled);
    }
	
    private final static class FakePDM implements ProcessorDataManager {
        private String expectedCommand;
        private boolean registerRequestCalled = false;
        private FakePDM(String command) {
            this.expectedCommand = command;
        }
        public Object lookupCodeValue(CommandContext context,String codeTableName,String returnElementName,String keyElementName,Object keyValue) throws BlockedException,MetaMatrixComponentException {return null;}
        public TupleSource registerRequest(Object processorID,Command command,String modelName,String connectorBindingId, int nodeID) throws MetaMatrixComponentException {
            registerRequestCalled = true;
            assertEquals(expectedCommand, command.toString());
            return null;
        }
        @Override
        public void clearCodeTables() {
        	
        }
    }
    
    public void testShouldExecuteUpdate() throws Exception {
        Update update = new Update();
        
        update.setGroup(new GroupSymbol("test")); //$NON-NLS-1$
        
        update.addChange(new ElementSymbol("e1"), new Constant("1")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertTrue(RelationalNodeUtil.shouldExecute(update, false));
        
        update.setChangeList(new SetClauseList());
        
        assertFalse(RelationalNodeUtil.shouldExecute(update, false));
    }
    
    public void testShouldExecuteLimitZero() throws Exception {
        Query query = (Query)QueryParser.getQueryParser().parseCommand("SELECT e1, e2 FROM pm1.g1 LIMIT 0"); //$NON-NLS-1$
        assertFalse(RelationalNodeUtil.shouldExecute(query, false));
    }
}
