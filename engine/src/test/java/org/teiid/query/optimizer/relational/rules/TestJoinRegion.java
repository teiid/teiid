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

import java.util.ArrayList;
import java.util.List;

import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.rules.JoinRegion;
import org.teiid.query.optimizer.relational.rules.RulePlanJoins;
import org.teiid.query.sql.lang.JoinType;


import junit.framework.TestCase;

public class TestJoinRegion extends TestCase {
    
    public void testFindJoinRegions() {
        
        List regions = new ArrayList();
        
        PlanNode joinRoot = TestFrameUtil.getExamplePlan();
        
        PlanNode joinRoot1 = TestFrameUtil.getExamplePlan();
        
        PlanNode outerJoin = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        
        outerJoin.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_LEFT_OUTER);
        
        outerJoin.addFirstChild(joinRoot);
        outerJoin.addFirstChild(joinRoot1);
        
        PlanNode source = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        
        source.addFirstChild(outerJoin);
        
        RulePlanJoins.findJoinRegions(source, null, regions);
        
        assertEquals(3, regions.size());
        
        JoinRegion region = (JoinRegion)regions.get(0);
        
        //ensure that the first region is the trivial region of the outer join
        assertEquals(1, region.getJoinSourceNodes().size());
    }

    public void testReconstruction() {
        
        List regions = new ArrayList();
        
        PlanNode joinRoot = TestFrameUtil.getExamplePlan();
        
        PlanNode source = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        
        source.addFirstChild(joinRoot);
        
        RulePlanJoins.findJoinRegions(source, null, regions);
        
        assertEquals(1, regions.size());
        
        JoinRegion region = (JoinRegion)regions.get(0);
        
        assertEquals(3, region.getJoinSourceNodes().size());
        
        assertEquals(joinRoot, region.getJoinRoot());
        
        region.changeJoinOrder(new Object[] {new Integer(1), new Integer(0), new Integer(2)});
        
        region.reconstructJoinRegoin();
        
        PlanNode root = region.getJoinRoot();
        
        assertEquals(NodeConstants.Types.JOIN, root.getFirstChild().getType());
        
        //the tree is now left linear so go down a couple of levels to get to the first source
        assertEquals(NodeConstants.Types.SOURCE, root.getFirstChild().getFirstChild().getFirstChild().getType());
    }
    
    /**
     * Simple test to ensure that the reconstruction logic doesn't fail with a single source
     */
    public void testReconstructionOf1Source() {
        
        PlanNode source = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        
        PlanNode accessNode = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        
        source.addFirstChild(accessNode);
        
        JoinRegion region = new JoinRegion();
        
        region.addJoinSourceNode(accessNode);
        
        region.reconstructJoinRegoin();
        
        assertEquals(NodeConstants.Types.ACCESS, region.getJoinRoot().getType());
    }
    
}
