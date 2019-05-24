/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.optimizer.relational.rules;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;


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
        QueryMetadataInterface metadata = new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore());
        Command command = TestOptimizer.helpGetCommand("select * from (select * from pm1.g1 union select * from pm1.g2) x where e1 = 1", metadata); //$NON-NLS-1$
        Command subCommand = TestOptimizer.helpGetCommand("select * from pm1.g1 union select * from pm1.g2", metadata); //$NON-NLS-1$
        RelationalPlanner p = new RelationalPlanner();
        CommandContext cc = new CommandContext();
        p.initialize(command, null, metadata, null, null, cc);
        PlanNode root = p.generatePlan(command);
        PlanNode child = p.generatePlan(subCommand);
        PlanNode sourceNode = NodeEditor.findNodePreOrder(root, NodeConstants.Types.SOURCE);
        sourceNode.addFirstChild(child);
        sourceNode.setProperty(NodeConstants.Info.SYMBOL_MAP, SymbolMap.createSymbolMap(sourceNode.getGroups().iterator().next(), (List<Expression>)child.getFirstChild().getProperty(Info.PROJECT_COLS), metadata));
        //add a dummy access node
        PlanNode accessNode = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        accessNode.addGroups(child.getFirstChild().getGroups());
        child.getFirstChild().addAsParent(accessNode);

        new RulePushSelectCriteria().execute(root, metadata, new DefaultCapabilitiesFinder(), new RuleStack(), AnalysisRecord.createNonRecordingRecord(), cc);
        // the select node should still be above the access node
        accessNode = NodeEditor.findNodePreOrder(root, NodeConstants.Types.ACCESS);
        assertEquals(NodeConstants.Types.SELECT, accessNode.getParent().getType());
        assertNull(NodeEditor.findNodePreOrder(accessNode, NodeConstants.Types.SELECT));
    }

}
