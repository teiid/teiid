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

package com.metamatrix.query.optimizer.relational.rules;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.optimizer.TestOptimizer;
import com.metamatrix.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.GenerateCanonical;
import com.metamatrix.query.optimizer.relational.PlanHints;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants.Info;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.util.CommandContext;

public class TestRulePushSelectCriteria {
    
    @Test public void testElementsInCritieria() throws Exception {
        String criteria = "e1 = '1' OR ((e1 = '2' OR e1 = '4') AND e2 = 3)"; //$NON-NLS-1$
        Set<ElementSymbol> expected = new HashSet<ElementSymbol>(Arrays.asList(new ElementSymbol("e1"))); //$NON-NLS-1$
        assertEquals(expected, RulePushSelectCriteria.getElementsIncriteria(QueryParser.getQueryParser().parseCriteria(criteria)));
    }
    
    @Test public void testElementsInCritieria1() throws Exception {
        String criteria = "e1 = '1' and ((e1 = '2' OR e1 = '4') AND e2 = 3) or e2 is null"; //$NON-NLS-1$
        Set<ElementSymbol> expected = new HashSet<ElementSymbol>(Arrays.asList(new ElementSymbol("e2"))); //$NON-NLS-1$
        assertEquals(expected, RulePushSelectCriteria.getElementsIncriteria(QueryParser.getQueryParser().parseCriteria(criteria)));
    }
    
    @Test public void testPushAcrossFrameWithAccessNode() throws Exception {
    	FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
    	Command command = TestOptimizer.helpGetCommand("select * from (select * from pm1.g1 union select * from pm1.g2) x where e1 = 1", metadata, null); //$NON-NLS-1$
    	Command subCommand = TestOptimizer.helpGetCommand("select * from pm1.g1 union select * from pm1.g2", metadata, null); //$NON-NLS-1$
    	PlanNode root = GenerateCanonical.generatePlan(command, new PlanHints(), metadata);
    	PlanNode child = GenerateCanonical.generatePlan(subCommand, new PlanHints(), metadata);
    	PlanNode sourceNode = NodeEditor.findNodePreOrder(root, NodeConstants.Types.SOURCE);
    	sourceNode.addFirstChild(child);
        sourceNode.setProperty(NodeConstants.Info.SYMBOL_MAP, SymbolMap.createSymbolMap(sourceNode.getGroups().iterator().next(), (List<SingleElementSymbol>)child.getFirstChild().getProperty(Info.PROJECT_COLS)));
    	//add a dummy access node
        PlanNode accessNode = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        accessNode.addGroups(child.getFirstChild().getGroups());
    	child.getFirstChild().addAsParent(accessNode);
    	
    	new RulePushSelectCriteria().execute(root, metadata, new DefaultCapabilitiesFinder(), new RuleStack(), AnalysisRecord.createNonRecordingRecord(), new CommandContext());
    	// the select node should still be above the access node
    	accessNode = NodeEditor.findNodePreOrder(root, NodeConstants.Types.ACCESS);
    	assertEquals(NodeConstants.Types.SELECT, accessNode.getParent().getType());
    	assertNull(NodeEditor.findNodePreOrder(accessNode, NodeConstants.Types.SELECT));
    }

}
