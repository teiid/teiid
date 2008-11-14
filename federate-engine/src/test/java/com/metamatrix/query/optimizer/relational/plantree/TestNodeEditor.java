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

package com.metamatrix.query.optimizer.relational.plantree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.query.sql.symbol.GroupSymbol;

/**
 */
public class TestNodeEditor extends TestCase {

    /**
     * Constructor for TestNodeEditor.
     * @param arg0
     */
    public TestNodeEditor(String arg0) {
        super(arg0);
    }

    // Helper to walk a tree and get the pre-order nodes of a tree
    public List getNodesPreOrder(PlanNode node) {
        ArrayList nodes = new ArrayList();
        helpGetNodesPreOrder(node, nodes);
        return nodes;
    }
    
    private void helpGetNodesPreOrder(PlanNode node, List collector) {
        collector.add(node);
        Iterator iter = node.getChildren().iterator();
        while(iter.hasNext()) {
            helpGetNodesPreOrder((PlanNode)iter.next(), collector);
        }
    }
    
    // Helper to build a tree from an array of names.  Each array holds a node name
    // and a list of either strings representing leaf children or Object[] holding
    // further subtrees.
    public PlanNode buildTree(Object[] nodeNames) {
        if(nodeNames == null) {
            return null;
        }
        PlanNode node = buildNamedNode((String)nodeNames[0]);
        for(int i=1; i<nodeNames.length; i++) {
            PlanNode childNode = null;
            if(nodeNames[i] instanceof String) {
                childNode = buildNamedNode((String)nodeNames[i]);
            } else {
                childNode = buildTree((Object[]) nodeNames[i]);
            }
            NodeEditor.attachLast(node, childNode);
        }
        return node;
    }
    
    private PlanNode buildNamedNode(String name) {
        PlanNode node = new PlanNode();
        node.addGroup(new GroupSymbol(name));
        return node;
    }
    
    public void assertTreesEqual(PlanNode tree1, PlanNode tree2) {
        assertEquals("Trees are not the same", tree1.toString(), tree2.toString()); //$NON-NLS-1$
    }
    
    public PlanNode exampleTree1() {
        return buildTree(new Object[] 
        { 
            "node_0",  //$NON-NLS-1$
            new Object[] { 
                "node_1",  //$NON-NLS-1$
                    "node_1_1", //$NON-NLS-1$
                    "node_1_2" //$NON-NLS-1$
            },
            new Object[] {
                "node_2", //$NON-NLS-1$
                    "node_2_1" //$NON-NLS-1$
            }
        }
        );
    }      
    
    // ############ BEGIN ACTUAL TESTS ###############
    
    public void testGetNoSibling() {
        PlanNode parent = new PlanNode();
        PlanNode child1 = new PlanNode();        
        NodeEditor.attachFirst(parent, child1);
        
        PlanNode sibling = NodeEditor.getSibling(child1);
        assertNull("Sibling should be null", sibling);   //$NON-NLS-1$
    }

    public void testGetSibling1() {
        PlanNode parent = new PlanNode();
        PlanNode child1 = new PlanNode();        
        PlanNode child2 = new PlanNode();
        NodeEditor.attachFirst(parent, child1);
        NodeEditor.attachLast(parent, child2);
        
        PlanNode sibling = NodeEditor.getSibling(child1);
        assertSame("Got wrong sibling", child2, sibling);   //$NON-NLS-1$
    }

    public void testGetSibling2() {
        PlanNode parent = new PlanNode();
        PlanNode child1 = new PlanNode();        
        PlanNode child2 = new PlanNode();
        NodeEditor.attachFirst(parent, child1);
        NodeEditor.attachLast(parent, child2);
        
        PlanNode sibling = NodeEditor.getSibling(child2);
        assertSame("Got wrong sibling", child1, sibling);   //$NON-NLS-1$
    }

    public void testTooManySiblings() {
        PlanNode parent = new PlanNode();
        PlanNode child1 = new PlanNode();        
        NodeEditor.attachFirst(parent, child1);
        NodeEditor.attachLast(parent, new PlanNode());
        NodeEditor.attachLast(parent, new PlanNode());
        
        try {
            NodeEditor.getSibling(child1);
            fail("Expected exception for too many siblings"); //$NON-NLS-1$
        } catch(AssertionError e) {           
        }
    }

    public void testGetSiblingNoParent() {
        PlanNode child1 = new PlanNode();        
        
        try {
            NodeEditor.getSibling(child1);
            fail("Expected exception for no parent"); //$NON-NLS-1$
        } catch(AssertionError e) {           
        }
    }
    
    public void testRemoveFirstChildNode() {
        PlanNode tree = exampleTree1();
        List expectedChildren = new ArrayList();
        expectedChildren.addAll(tree.getFirstChild().getChildren());
        expectedChildren.add(tree.getLastChild());

        NodeEditor.removeChildNode(tree, tree.getFirstChild());        
        List actualChildren = tree.getChildren();
        
        assertEquals("Didn't get expected children after removing first child", expectedChildren, actualChildren); //$NON-NLS-1$
    }

    public void testRemoveLastChildNode() {
        PlanNode tree = exampleTree1();
        List expectedChildren = new ArrayList();
        expectedChildren.add(tree.getFirstChild());
        expectedChildren.addAll(tree.getLastChild().getChildren());

        NodeEditor.removeChildNode(tree, tree.getLastChild());        
        List actualChildren = tree.getChildren();
        
        assertEquals("Didn't get expected children after removing last child", expectedChildren, actualChildren); //$NON-NLS-1$
    }

    public void testRemoveBothChildNodes1() {
        PlanNode tree = exampleTree1();
        List expectedChildren = new ArrayList();
        expectedChildren.addAll(tree.getFirstChild().getChildren());
        expectedChildren.addAll(tree.getLastChild().getChildren());

        NodeEditor.removeChildNode(tree, tree.getLastChild());        
        NodeEditor.removeChildNode(tree, tree.getFirstChild());        
        List actualChildren = tree.getChildren();
        
        assertEquals("Didn't get expected children after removing last then first", expectedChildren, actualChildren); //$NON-NLS-1$
    }

    public void testRemoveBothChildNodes2() {
        PlanNode tree = exampleTree1();
        List expectedChildren = new ArrayList();
        expectedChildren.addAll(tree.getFirstChild().getChildren());
        expectedChildren.addAll(tree.getLastChild().getChildren());

        NodeEditor.removeChildNode(tree, tree.getFirstChild());        
        NodeEditor.removeChildNode(tree, tree.getLastChild());        
        List actualChildren = tree.getChildren();
        
        assertEquals("Didn't get expected children after removing first then last", expectedChildren, actualChildren); //$NON-NLS-1$
    }
    
    public void testReplaceNode1() {        
        PlanNode tree = exampleTree1();
        PlanNode original = tree.getLastChild();
        PlanNode replacement = buildNamedNode("r"); //$NON-NLS-1$
        PlanNode expectedTree = buildTree(new Object[] 
        { 
            "node_0", //$NON-NLS-1$
            new Object[] { 
                "node_1",  //$NON-NLS-1$
                    "node_1_1", //$NON-NLS-1$
                    "node_1_2" //$NON-NLS-1$
            },
            new Object[] {
                "r", //$NON-NLS-1$
                    "node_2_1" //$NON-NLS-1$
            }
        }
        );
        
        NodeEditor.replaceNode(original, replacement);
        assertTreesEqual(expectedTree, tree);
    }

    public void testReplaceNode2() {        
        PlanNode tree = exampleTree1();
        PlanNode original = tree.getFirstChild();
        PlanNode replacement = buildNamedNode("r"); //$NON-NLS-1$
        PlanNode expectedTree = buildTree(new Object[] 
        { 
            "node_0", //$NON-NLS-1$
            new Object[] {
                "r", //$NON-NLS-1$
                    "node_1_1", //$NON-NLS-1$
                    "node_1_2",  //$NON-NLS-1$
            },
            new Object[] {
                "node_2", //$NON-NLS-1$
                    "node_2_1" //$NON-NLS-1$
            }
        }
        );
        
        NodeEditor.replaceNode(original, replacement);
        assertTreesEqual(expectedTree, tree);
    }
    
    public void testFindNodePreOrder1() {               
        PlanNode node1 = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
        PlanNode node2 = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        PlanNode node3 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        PlanNode node4 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        
        NodeEditor.attachLast(node1, node2);
        NodeEditor.attachLast(node2, node3);
        NodeEditor.attachLast(node2, node4);
        
        assertEquals("Found wrong node", node1, NodeEditor.findNodePreOrder(node1, NodeConstants.Types.PROJECT)); //$NON-NLS-1$
        assertEquals("Found wrong node", node2, NodeEditor.findNodePreOrder(node1, NodeConstants.Types.JOIN)); //$NON-NLS-1$
        assertEquals("Found wrong node", node3, NodeEditor.findNodePreOrder(node1, NodeConstants.Types.ACCESS)); //$NON-NLS-1$
        assertEquals("Found wrong node", null, NodeEditor.findNodePreOrder(node1, NodeConstants.Types.GROUP));         //$NON-NLS-1$
        assertEquals("Found wrong node", null, NodeEditor.findNodePreOrder(node1, NodeConstants.Types.ACCESS, NodeConstants.Types.JOIN)); //$NON-NLS-1$
    }
    
    public void testFindParent() {               
        PlanNode node0 = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        PlanNode node1 = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
        PlanNode node2 = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        PlanNode node3 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        PlanNode node4 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        NodeEditor.attachLast(node0, node1);
        NodeEditor.attachLast(node1, node2);
        NodeEditor.attachLast(node2, node3);
        NodeEditor.attachLast(node2, node4);
        
        assertEquals("Found wrong node", node1, NodeEditor.findParent(node4, NodeConstants.Types.PROJECT)); //$NON-NLS-1$
        assertNull("Found wrong node", NodeEditor.findParent(node1, NodeConstants.Types.PROJECT)); //$NON-NLS-1$
        assertNull("Found wrong node", NodeEditor.findParent(node4, NodeConstants.Types.PROJECT, NodeConstants.Types.JOIN|NodeConstants.Types.SELECT)); //$NON-NLS-1$
        assertNull("Found wrong node", NodeEditor.findParent(node1, NodeConstants.Types.GROUP));         //$NON-NLS-1$
    }
}
