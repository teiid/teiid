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

package com.metamatrix.console.ui.views.entitlements;

import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ui.tree.SortsChildren;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticTreeUtilities;
import com.metamatrix.core.util.ResourceNameUtil;
import com.metamatrix.platform.admin.api.PermissionNode;
import com.metamatrix.platform.admin.api.PermissionTreeView;
import com.metamatrix.toolbox.ui.widget.CheckBox;

public class DataNodesTreeModel extends javax.swing.tree.DefaultTreeModel
        implements AuthorizationPropagationListener {
    private final static double TIME_REPORTING_THRESHOLD = 2.0;
    
    public static DataNodesTreeModel createDefaultTreeModelFromTreeView(
            PermissionTreeView treeView, boolean canModifyEntitlements,
            boolean usingRoot) throws Exception {
        Date startingTime = new Date();
        com.metamatrix.common.tree.TreeNode root =
                treeView.getRoot();
        DataNodesTreeNode ourRoot;
        if (usingRoot) {
            ourRoot = setUpNode(treeView, root, canModifyEntitlements);
            ourRoot.setRootNode(true);
            if (ourRoot.getNodeDisplayName().trim().length() == 0) {
                ourRoot.setNodeDisplayName("Repository root");
            }
        } else {
            ourRoot = new DataNodesTreeNode("root", "root",
                	root, true, -1, -1, -1, -1, canModifyEntitlements);
        }
        ourRoot.setSortType(SortsChildren.NO_SORT);
        DataNodesTreeModel ourModel = new DataNodesTreeModel(ourRoot);
        com.metamatrix.common.tree.TreeNode curNode = root;
        DataNodesTreeNode ourCurNode = ourRoot;
        boolean done = false;
        while (!done) {
//PermissionNode curNodePDN = (PermissionNode)curNode;
//AuthorizationActions allowed = curNodePDN.getAllowedActions();
//String[] allowedLabels = allowed.getLabels();
//String allowedLabelsStr = "";
//for (int ix = 0; ix < allowedLabels.length; ix++) {
// allowedLabelsStr += " " + allowedLabels[ix];
//}
//AuthorizationActions actual = curNodePDN.getActions();
//String[] actualLabels = actual.getLabels();
//String actualLabelsStr = "";
//for (int ix = 0; ix < actualLabels.length; ix++) {
// actualLabelsStr += " " + actualLabels[ix];
//}	

            java.util.List children = treeView.getChildren((PermissionNode)curNode);
            if (children.size() > 0) {
                curNode = (com.metamatrix.common.tree.TreeNode)children.get(0);
                DataNodesTreeNode ourChild = setUpNode(treeView, curNode,
                        canModifyEntitlements);
                ourCurNode.add(ourChild);
                ourCurNode = ourChild;
            } else {
                //Need to find next node.  This will be another child node of an
                //ancestor to the current node.
                boolean nextNodeFound = false;
                while ((!nextNodeFound) && (!done)) {
                    if (ourCurNode.isRootNode()) {
                        done = true;
                    } else {
                        com.metamatrix.common.tree.TreeNode parent =
                                treeView.getParent((PermissionNode)curNode);
                        DataNodesTreeNode ourParent =
                                (DataNodesTreeNode)ourCurNode.getParent();
                        int numChildrenAddedSoFar = ourParent.getChildCount();
                        java.util.List allChildrenForNode =
                                treeView.getChildren((PermissionNode)parent);
                        if (numChildrenAddedSoFar < allChildrenForNode.size()) {
                            curNode = (com.metamatrix.common.tree.TreeNode)
                                    allChildrenForNode.get(numChildrenAddedSoFar);
                            ourCurNode = setUpNode(treeView, curNode, 
                                    canModifyEntitlements);
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
        //NOTE-- the following call will remove from the tree model any nodes
        //we don't want there.  More efficient, obviously, would be to modify
        //the above code to not put unwanted nodes in the model in the first
        //place.  But the new method is quicker to implement, and deadlines
        //loom large.  BWP  11/10/01
        removeUnwantedNodes(ourRoot);

        sortTheModel(ourModel);
        Date endingTime = new Date();
        long startingTimeLong = startingTime.getTime();
        long endingTimeLong = endingTime.getTime();
        long timeDiffLong = endingTimeLong - startingTimeLong;
        double timeDiff = ((double)timeDiffLong) / 1000;
        if (timeDiff >= TIME_REPORTING_THRESHOLD) {
            java.util.List tempList = StaticTreeUtilities.descendantsOfNode(
                    ourRoot, true);
            int numNodes = tempList.size();
            String message = "Time duration in DataNodesTreeModel.." +
                    "createDefaultTreeModelFromTreeView() is " + timeDiff +
                    " seconds for model containing " + numNodes + " nodes";
            LogManager.logInfo(LogContexts.ENTITLEMENTS, message);
        }
        return ourModel;
    }

    private static void removeUnwantedNodes(DataNodesTreeNode root) {
        java.util.List /*<DataNodesTreeNode>*/ nodes =
                StaticTreeUtilities.descendantsOfNode(root, false);
        Iterator it = nodes.iterator();
        while (it.hasNext()) {
            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
            String displayName = node.getNodeDisplayName();
            if (displayName.equals(ResourceNameUtil.SYSTEMPHYSICAL_NAME) ||
                  //  MetadataConstants.RUNTIME_MODEL.PHYSICAL_MODEL_NAME) ||
                displayName.equals(ResourceNameUtil.SYSTEMADMINPHYSICAL_NAME)) {
                                   //MetadataConstants.RUNTIME_MODEL.ADMIN_PHYSICAL_MODEL_NAME)) {
                node.removeFromParent();
                break;
            }
        }
    }

    private static DataNodesTreeNode setUpNode(PermissionTreeView treeView,
            com.metamatrix.common.tree.TreeNode curNode, boolean canModify) {
        PermissionNode pdn = (PermissionNode)curNode;
        String displayName = pdn.getDisplayName();
        String resourceName = pdn.getResourceName();
        int createState = CheckBox.DESELECTED;
        int readState = CheckBox.DESELECTED;
        int updateState = CheckBox.DESELECTED;
        int deleteState = CheckBox.DESELECTED;
        String[] permissions = pdn.getActions().getLabels();
        for (int i = 0; i < permissions.length; i++) {
            if (permissions[i].equalsIgnoreCase("Create")) {
                createState = CheckBox.SELECTED;
            } else if (permissions[i].equalsIgnoreCase("Read")) {
                readState = CheckBox.SELECTED;
            } else if (permissions[i].equalsIgnoreCase("Update")) {
                updateState = CheckBox.SELECTED;
            } else if (permissions[i].equalsIgnoreCase("Delete")) {
                deleteState = CheckBox.SELECTED;
            }
        }
        DataNodesTreeNode theNode = new DataNodesTreeNode(displayName,
                resourceName, curNode, false, createState, readState, 
                updateState, deleteState, canModify);
        theNode.setSortType(SortsChildren.NO_SORT);
        return theNode;
    }

    private static void sortTheModel(DataNodesTreeModel model) {
        java.util.List /*<DataNodesTreeNode>*/ nodes =
                StaticTreeUtilities.descendantsOfNode(
                (DataNodesTreeNode)model.getRoot(), true);
        Iterator it = nodes.iterator();
        while (it.hasNext()) {
            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
            node.setSortType(SortsChildren.ALPHABETIC_SORT);
//            boolean changed =
                node.sortChildren();
        }
    }
    
    private AuthorizationPropagationListener propagationListener;
    private boolean usingRoot;

    public DataNodesTreeModel(boolean usingRoot, int rootCreateState,
    		int rootReadState, int rootUpdateState, int rootDeleteState) {
        super(new DataNodesTreeNode("root", "root", null, true, 
        		rootCreateState, rootReadState, rootUpdateState, rootDeleteState, 
        		false));
        this.usingRoot = usingRoot;
    }

    public DataNodesTreeModel(DataNodesTreeNode root) {
        super(root);
    }

    /**
     * Change the state to checked for a given column for each node for which all
     * descendant nodes have the flag set.  For purposes of doing this we will
     * ignore unset leaf nodes in the "delete" column, since these cannot be
     * set.
     */
    public void setStatesToChecked() {
        //Each node will recursively recalculate the state for each column for
        //each descendant node, then use those recalculated states in
        //recalculating its own states.
        if (usingRoot) {
            DataNodesTreeNode root = (DataNodesTreeNode)this.getRoot();
            root.recalculateStates();
        } else {
            Enumeration rootsChildren = 
            		((DataNodesTreeNode)this.getRoot()).children();
            while (rootsChildren.hasMoreElements()) {
                DataNodesTreeNode rootsChild = 
                		(DataNodesTreeNode)rootsChildren.nextElement();
                rootsChild.recalculateStates();
            }
        }
    }
    
    public void setStatesToPartial() {
        DataNodesTreeNode root = (DataNodesTreeNode)this.getRoot();
        java.util.List /*<DataNodesTreeNode>*/ nodes = 
        		StaticTreeUtilities.descendantsOfNode(root, this.usingRoot);
        Iterator it = nodes.iterator();
        while (it.hasNext()) {
            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
            for (int i = 0; i <= 3; i++) {
                int col = -1;
                boolean selected = false;
                switch (i) {
                    case 0:
                        selected = (node.getCreateState() ==
                                CheckBox.SELECTED);
                        col = AuthorizationsModel.CREATE_COLUMN_NUM;
                        break;
                    case 1:
                        selected = (node.getReadState() ==
                                CheckBox.SELECTED);
                        col = AuthorizationsModel.READ_COLUMN_NUM;
                        break;
                    case 2:
                        selected = (node.getUpdateState() ==
                                CheckBox.SELECTED);
                        col = AuthorizationsModel.UPDATE_COLUMN_NUM;
                        break;
                    case 3:
                        selected = (node.getDeleteState() ==
                                CheckBox.SELECTED);
                        col = AuthorizationsModel.DELETE_COLUMN_NUM;
                        break;
                }
                boolean changeToPartial;
                if (selected) {
                    changeToPartial = node.hasDescendantUnauthorized(col);
                } else {
                    changeToPartial = node.hasDescendantAuthorized(col);
                }
                if (changeToPartial) {
                    switch (col) {
                        case AuthorizationsModel.CREATE_COLUMN_NUM:
                            node.setCreateState(CheckBox.PARTIALLY_SELECTED);
                            node.setInitialCreateState(CheckBox.PARTIALLY_SELECTED);
                            break;
                        case AuthorizationsModel.READ_COLUMN_NUM:
                            node.setReadState(CheckBox.PARTIALLY_SELECTED);
                            node.setInitialReadState(CheckBox.PARTIALLY_SELECTED);
                            break;
                        case AuthorizationsModel.UPDATE_COLUMN_NUM:
                            node.setUpdateState(CheckBox.PARTIALLY_SELECTED);
                            node.setInitialUpdateState(CheckBox.PARTIALLY_SELECTED);
                            break;
                        case AuthorizationsModel.DELETE_COLUMN_NUM:
                            node.setDeleteState(CheckBox.PARTIALLY_SELECTED);
                            node.setInitialDeleteState(CheckBox.PARTIALLY_SELECTED);
                            break;
                    }
                }
            }
        }
    }

    public void setPropagationListener(AuthorizationPropagationListener lsnr) {
        propagationListener = lsnr;
    }

    public void authorizationPropagated(DataNodesTreeNode node,
            int authorizationType) {
        if (propagationListener != null) {
            propagationListener.authorizationPropagated(node, authorizationType);
        }
    }

    public void setTreeTableForEachNode(AuthorizationsTreeTable treeTable) {
        java.util.List /*<DataNodesTreeNode>*/ nodes =
                StaticTreeUtilities.descendantsOfNode(
                (DataNodesTreeNode)this.getRoot(), true);
        Iterator it = nodes.iterator();
        while (it.hasNext()) {
            DataNodesTreeNode node = (DataNodesTreeNode)it.next();
            node.setTreeTable(treeTable);
        }
    }
}
