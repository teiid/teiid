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

import java.util.Arrays;

import org.junit.Test;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;



/** 
 * @since 4.2
 */
public class TestAccessNode {

    private void helpTestOpen(Command command, String expectedCommand, boolean shouldRegisterRequest) throws Exception {
        // Setup
        AccessNode node = new AccessNode(1);
        node.setCommand(command);
        CommandContext context = new CommandContext();
        context.setProcessorID("processorID"); //$NON-NLS-1$
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        FakeDataManager dataManager = new FakeDataManager(); 
        TestProcessor.sampleData1(dataManager);
        node.setElements(command.getProjectedSymbols());
        node.initialize(context, bm, dataManager);
        node.setShouldEvaluateExpressions(true);
        // Call open()
        node.open();
        if (shouldRegisterRequest) {
        	assertEquals(Arrays.asList(expectedCommand), dataManager.getQueries());
        } else {
        	assertEquals(0, dataManager.getQueries().size());
        }
    }
    
    @Test public void testOpen_Defect16059() throws Exception {
    	Query query = (Query)TestResolver.helpResolve("SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5 AND ? IS NULL", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
        IsNullCriteria nullCrit = (IsNullCriteria)((CompoundCriteria)query.getCriteria()).getCriteria(1);
        nullCrit.setExpression(new Constant(null));
        
        helpTestOpen(query, "SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5", true); //$NON-NLS-1$
    }
    
    @Test public void testOpen_Defect16059_2() throws Exception {
    	Query query = (Query)TestResolver.helpResolve("SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5 AND ? IS NOT NULL", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
        IsNullCriteria nullCrit = (IsNullCriteria)((CompoundCriteria)query.getCriteria()).getCriteria(1);
        nullCrit.setExpression(new Constant(null));
        
        helpTestOpen(query, null, false);
    }
    
    @Test public void testExecCount()throws Exception{
        // Setup
        AccessNode node = new AccessNode(1);
    	Query query = (Query)TestResolver.helpResolve("SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
        node.setCommand(query);
        CommandContext context = new CommandContext();
        context.setProcessorID("processorID"); //$NON-NLS-1$
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        FakeDataManager dataManager = new FakeDataManager(); 
        TestProcessor.sampleData1(dataManager);
        node.setElements(query.getProjectedSymbols());
        node.initialize(context, bm, dataManager);
        // Call open()
        node.open();
        assertEquals(Arrays.asList("SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5"), dataManager.getQueries()); //$NON-NLS-1$
    }
	
    @Test public void testShouldExecuteUpdate() throws Exception {
        Update update = new Update();
        
        update.setGroup(new GroupSymbol("test")); //$NON-NLS-1$
        
        update.addChange(new ElementSymbol("e1"), new Constant("1")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertTrue(RelationalNodeUtil.shouldExecute(update, false));
        
        update.setChangeList(new SetClauseList());
        
        assertFalse(RelationalNodeUtil.shouldExecute(update, false));
    }
    
    @Test public void testShouldExecuteLimitZero() throws Exception {
        Query query = (Query)QueryParser.getQueryParser().parseCommand("SELECT e1, e2 FROM pm1.g1 LIMIT 0"); //$NON-NLS-1$
        assertFalse(RelationalNodeUtil.shouldExecute(query, false));
    }
}
