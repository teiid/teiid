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

package org.teiid.query.optimizer.relational.rules;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.sql.lang.SetQuery.Operation;
/**
 */
public class TestRuleAssignOutputElements {

    public void helpTestIsUnionNoAll(PlanNode node, boolean expected) {
        boolean actual = RuleAssignOutputElements.hasDupRemoval(node);
        assertEquals("Got incorrect answer finding no all union", expected, actual); //$NON-NLS-1$
    }

    @Test public void testFindNoAllUnion1() {
        PlanNode projNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);        
        PlanNode accessNode = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);        
        projNode.addLastChild(accessNode);
        
        helpTestIsUnionNoAll(projNode, false);
    }    

    @Test public void testFindNoAllUnion2() {
        PlanNode unionNode = NodeFactory.getNewNode(NodeConstants.Types.SET_OP);
        unionNode.setProperty(NodeConstants.Info.SET_OPERATION, Operation.UNION);
        unionNode.setProperty(NodeConstants.Info.USE_ALL, Boolean.TRUE);
        PlanNode projNode1 = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);        
        PlanNode accessNode1 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);        
        PlanNode projNode2 = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);        
        PlanNode accessNode2 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);        

        unionNode.addLastChild(projNode1);
        projNode1.addLastChild(accessNode1);
        unionNode.addLastChild(projNode2);
        projNode2.addLastChild(accessNode2);
        
        helpTestIsUnionNoAll(unionNode, false);
    }    
    
    @Test public void testFindNoAllUnion3() {
        PlanNode unionNode = NodeFactory.getNewNode(NodeConstants.Types.SET_OP);
        unionNode.setProperty(NodeConstants.Info.SET_OPERATION, Operation.UNION);
        unionNode.setProperty(NodeConstants.Info.USE_ALL, Boolean.FALSE);
        PlanNode projNode1 = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);        
        PlanNode accessNode1 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);        
        PlanNode projNode2 = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);        
        PlanNode accessNode2 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);        

        unionNode.addLastChild(projNode1);
        projNode1.addLastChild(accessNode1);
        unionNode.addLastChild(projNode2);
        projNode2.addLastChild(accessNode2);
        
        helpTestIsUnionNoAll(unionNode, true);
    }    

    @Test public void testFindNoAllUnion4() {
        PlanNode unionNode1 = NodeFactory.getNewNode(NodeConstants.Types.SET_OP);
        unionNode1.setProperty(NodeConstants.Info.SET_OPERATION, Operation.UNION);
        unionNode1.setProperty(NodeConstants.Info.USE_ALL, Boolean.TRUE);
        PlanNode unionNode2 = NodeFactory.getNewNode(NodeConstants.Types.SET_OP);
        unionNode2.setProperty(NodeConstants.Info.SET_OPERATION, Operation.UNION);
        unionNode2.setProperty(NodeConstants.Info.USE_ALL, Boolean.FALSE);
        PlanNode projNode1 = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);        
        PlanNode accessNode1 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);        
        PlanNode projNode2 = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);        
        PlanNode accessNode2 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);        
        PlanNode projNode3 = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);        
        PlanNode accessNode3 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);        

        unionNode1.addLastChild(projNode1);
        projNode1.addLastChild(accessNode1);
        unionNode1.addLastChild(unionNode2);
        unionNode2.addLastChild(projNode2);
        projNode2.addLastChild(accessNode2);
        unionNode2.addLastChild(projNode3);
        projNode3.addLastChild(accessNode3);
        
        helpTestIsUnionNoAll(unionNode1, true);
    }    

}
