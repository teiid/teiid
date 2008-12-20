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

package com.metamatrix.query.optimizer.relational.rules;

import java.util.HashSet;
import java.util.Set;

import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.symbol.GroupSymbol;

import junit.framework.TestCase;

public class TestFrameUtil extends TestCase {
    
    static GroupSymbol getGroup(int id) {
        return new GroupSymbol(String.valueOf(id));
    }
    
    public void testFindJoinSourceNode() {
        PlanNode root = getExamplePlan();
        
        PlanNode joinSource = FrameUtil.findJoinSourceNode(root);
        
        assertSame(root, joinSource);
    }
    
    public void testFindJoinSourceNode1() {
        PlanNode root = getExamplePlan();
        
        PlanNode joinSource = FrameUtil.findJoinSourceNode(root.getLastChild());
        
        assertEquals(NodeConstants.Types.JOIN, joinSource.getType());
    }
    
    public void testFindSourceNode() {
        PlanNode root = getExamplePlan();
                
        Set groups = new HashSet();
        
        groups.add(getGroup(1));
        
        PlanNode originatingNode = FrameUtil.findOriginatingNode(root, groups);
        
        assertEquals(NodeConstants.Types.NULL, originatingNode.getType());
    }

    /**
     * Access nodes are not eligible originating nodes
     */
    public void testFindSourceNodeWithAccessSource() {
        PlanNode root = getExamplePlan();
                
        Set groups = new HashSet();
        
        groups.add(getGroup(2));
        
        PlanNode originatingNode = FrameUtil.findOriginatingNode(root, groups);
        
        assertEquals(NodeConstants.Types.JOIN, originatingNode.getType());
    }

    public void testFindSourceNode2() {
        PlanNode root = getExamplePlan();
                
        Set groups = new HashSet();
        
        groups.add(getGroup(3));
        
        PlanNode originatingNode = FrameUtil.findOriginatingNode(root, groups);
        
        assertEquals(NodeConstants.Types.SOURCE, originatingNode.getType());
    }
    
    public void testNonExistentSource() {
        PlanNode root = getExamplePlan();
        
        Set groups = new HashSet();
        
        groups.add(getGroup(4));
        
        PlanNode originatingNode = FrameUtil.findOriginatingNode(root, groups);
        
        assertNull(originatingNode);
    }

    /**
     * <pre>
     * Join(groups=[3, 2, 1])
     *   Null(groups=[1])
     *   Select(groups=[2])
     *     Join(groups=[3, 2])
     *       Source(groups=[3])
     *       Access(groups=[2])
     * </pre>
     */
    public static PlanNode getExamplePlan() {
        PlanNode joinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_CROSS);
        joinNode.addGroup(getGroup(1)); 
        joinNode.addGroup(getGroup(2)); 
        joinNode.addGroup(getGroup(3)); 
        
        PlanNode nullNode = NodeFactory.getNewNode(NodeConstants.Types.NULL);
        
        nullNode.addGroup(getGroup(1)); 
        joinNode.addFirstChild(nullNode);
        
        PlanNode childCriteria = NodeFactory.getNewNode(NodeConstants.Types.SELECT);

        childCriteria.addGroup(getGroup(2));
        joinNode.addLastChild(childCriteria);
        
        PlanNode childJoinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        childJoinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_CROSS);
        childJoinNode.addGroup(getGroup(2)); 
        childJoinNode.addGroup(getGroup(3)); 
        childCriteria.addFirstChild(childJoinNode);
        
        PlanNode accessNode = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        
        accessNode.addGroup(getGroup(2)); 
        childJoinNode.addFirstChild(accessNode);
        
        PlanNode sourceNode = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        
        sourceNode.addGroup(getGroup(3)); 
        childJoinNode.addFirstChild(sourceNode);
        
        return joinNode;
    }
}
