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

package org.teiid.query.optimizer.relational.plantree;

import java.util.ArrayList;
import java.util.List;

import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.sql.symbol.GroupSymbol;

import junit.framework.TestCase;


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
            node.addLastChild(childNode);
        }
        return node;
    }
    
    private PlanNode buildNamedNode(String name) {
        PlanNode node = new PlanNode();
        node.addGroup(new GroupSymbol(name));
        return node;
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
    
    public void testRemoveLastChildNode() {
        PlanNode tree = exampleTree1();
        List expectedChildren = new ArrayList();
        expectedChildren.add(tree.getFirstChild());
        expectedChildren.addAll(tree.getLastChild().getChildren());

        NodeEditor.removeChildNode(tree, tree.getLastChild());        
        List actualChildren = tree.getChildren();
        
        assertEquals("Didn't get expected children after removing last child", expectedChildren, actualChildren); //$NON-NLS-1$
    }

    public void testFindNodePreOrder1() {               
        PlanNode node1 = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
        PlanNode node2 = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        PlanNode node3 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        PlanNode node4 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        
        node1.addLastChild(node2);
        node2.addLastChild(node3);
        node2.addLastChild(node4);
        
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
        node0.addLastChild(node1);
        node1.addLastChild(node2);
        node2.addLastChild(node3);
        node2.addLastChild(node4);
        
        assertEquals("Found wrong node", node1, NodeEditor.findParent(node4, NodeConstants.Types.PROJECT)); //$NON-NLS-1$
        assertNull("Found wrong node", NodeEditor.findParent(node1, NodeConstants.Types.PROJECT)); //$NON-NLS-1$
        assertNull("Found wrong node", NodeEditor.findParent(node4, NodeConstants.Types.PROJECT, NodeConstants.Types.JOIN|NodeConstants.Types.SELECT)); //$NON-NLS-1$
        assertNull("Found wrong node", NodeEditor.findParent(node1, NodeConstants.Types.GROUP));         //$NON-NLS-1$
    }
}
