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

package com.metamatrix.console.ui.views.vdb;

import java.awt.Color;
import java.awt.Component;

import javax.swing.Icon;
import javax.swing.JLabel;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;

import com.metamatrix.console.ui.util.ConsoleCellRenderer;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.platform.admin.api.PermissionDataNode;
import com.metamatrix.platform.admin.api.PermissionDataNodeDefinition;
import com.metamatrix.platform.admin.api.PermissionTreeView;


public class DataNodesTreeModel extends javax.swing.tree.DefaultTreeModel {
    
    /**
     * @param root
     */
    public DataNodesTreeModel(javax.swing.tree.TreeNode root) {
        super(root);
    }

    /**
     * @param root
     * @param asksAllowsChildren
     */
    public DataNodesTreeModel(javax.swing.tree.TreeNode root, boolean asksAllowsChildren) {
        super(root, asksAllowsChildren);
    }

    
    public static DefaultTreeCellRenderer createCellRenderer() {
        return new MetadataTreeCellRenderer();
    }
    
    public static DataNodesTreeModel createDefaultTreeModelFromTreeView(
            PermissionTreeView treeView, DataNodesTreeModel ourModel, DefaultMutableTreeNode treeRoot) throws Exception {
        PermissionDataNode root = 
                (PermissionDataNode)treeView.getRoot();
                

        PermissionDataNode curNode = null;
        VDBMetadataTreeNode ourCurNode = null;
        boolean done = false;
        while (!done) {

            java.util.List children = null;
            if (curNode != null) {
                children = treeView.getChildren(curNode);
            } else {                
                children = treeView.getChildren(root);
            } 

             
            if (children.size() > 0) {
                curNode = (PermissionDataNode)children.get(0);
                String cname = curNode.getName();
                VDBMetadataTreeNode ourChild = new VDBMetadataTreeNode(cname,curNode);

                if (ourCurNode != null) {
                    ourCurNode.add(ourChild);

                } else {
                    treeRoot.add(ourChild);
                    
                }
                ourCurNode = ourChild;
            } else {
                //Need to find next node.  This will be another child node of an
                //ancestor to the current node.
                boolean nextNodeFound = false;
                while ((!nextNodeFound) && (!done)) {
                    if (curNode == root) {
                        done = true;
                    } else {
                                               
                        PermissionDataNode parent = (PermissionDataNode) treeView.getParent(curNode);
                        VDBMetadataTreeNode ourParent =
                                (VDBMetadataTreeNode)ourCurNode.getParent();
                        int numChildrenAddedSoFar = ourParent.getChildCount();
                        java.util.List allChildrenForNode =
                                treeView.getChildren(parent);
                        if (numChildrenAddedSoFar < allChildrenForNode.size()) {
                            curNode = (PermissionDataNode)
                                    allChildrenForNode.get(numChildrenAddedSoFar);
                               
                            ourCurNode = new VDBMetadataTreeNode(curNode.getName(),curNode);
                                    

                            ourParent.add(ourCurNode);
                            nextNodeFound = true;
                        } else {
                            curNode = parent;
                            ourCurNode = ourParent;
                        }
                    }
                }
            }
        }
        return ourModel;
    }



//
//    private static void sortTheModel(DataNodesTreeModel model) {
//        java.util.List /*<DataNodesTreeNode>*/ nodes =
//                StaticTreeUtilities.descendantsOfNode(
//                (DataNodesTreeNode)model.getRoot(), true);
//        Iterator it = nodes.iterator();
//        while (it.hasNext()) {
//            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
//            node.setSortType(SortsChildren.ALPHABETIC_SORT);
////            boolean changed =
//                node.sortChildren();
//        }
//    }
    

    
}




class MetadataTreeCellRenderer extends DefaultTreeCellRenderer {
    public final static Color XML_DOCUMENT_FOREGROUND_COLOR = 
            StaticUtilities.averageRGBVals(new Color[] {Color.black,
            Color.red});
    public final static Color STORED_PROCEDURE_FOREGROUND_COLOR = Color.blue;

//Constructors

    /**
     * Constructor.
     */
    public MetadataTreeCellRenderer() {
        super();
    }

//Overridden methods

    public Component getTreeCellRendererComponent(
            final JTree tree,
            final Object value,
            boolean isSelected,
            final boolean isExpanded,
            final boolean isLeaf,
            final int row,
            final boolean hasFocus) {

        JLabel comp = (JLabel)super.getTreeCellRendererComponent(
                    tree, value, isSelected, isExpanded, isLeaf, row, hasFocus);
        if (value != null) {
            VDBMetadataTreeNode node = (VDBMetadataTreeNode)value;
            Icon icon = null;
            PermissionDataNode pNode =
                    node.getCorrespondingTreeNode();
            if (pNode != null) {
                int type = pNode.getDataNodeType();
                if (type == PermissionDataNodeDefinition.TYPE.PROCEDURE) {
                    icon = ConsoleCellRenderer.STORED_PROCEDURE_ICON;
                    comp.setForeground(STORED_PROCEDURE_FOREGROUND_COLOR);
                } else if (type == PermissionDataNodeDefinition.TYPE.DOCUMENT) {
                    icon = ConsoleCellRenderer.XML_DOCUMENT_ICON;
                    comp.setForeground(XML_DOCUMENT_FOREGROUND_COLOR);
                } else { 
                    if (node.parentIsRoot()) {
                        boolean isPhysicalModel = pNode.isPhysical();
                        if (isPhysicalModel) {
                            icon = ConsoleCellRenderer.PHYSICAL_MODEL_ICON;
                        } else {
                            icon = ConsoleCellRenderer.VIRTUAL_MODEL_ICON;
                        }
                    } else {
                        boolean isAttribute = (node.getChildCount() == 0);
                        if (isAttribute) {
                            icon = ConsoleCellRenderer.ATTRIBUTE_ICON;
                        }
                    }
                }
            }
            comp.setIcon(icon);
        }
        return comp;
    }
}//end MetadataTreeCellRenderer
