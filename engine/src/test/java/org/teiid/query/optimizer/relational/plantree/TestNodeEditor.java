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
