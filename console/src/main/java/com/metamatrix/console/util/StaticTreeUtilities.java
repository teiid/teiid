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

package com.metamatrix.console.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JTabbedPane;
import javax.swing.JTree;
import javax.swing.event.TreeExpansionListener;
import javax.swing.event.TreeSelectionListener;
import javax.swing.event.TreeWillExpandListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.tree.TreeView;
import com.metamatrix.console.ui.tree.TreePathExpansion;
import com.metamatrix.console.ui.util.LazyBranchNode;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeNode;

/**
 * Useful static methods that deal with trees.
 */
public class StaticTreeUtilities {
    
    // Context and ErrorLevel for dupe checking the tree
    public static final String CONTEXT_FOR_DUPE_CHECKING = LogContexts.USERS; 
    public static final int MSGLEVEL_FOR_DUPE_CHECKING = MessageLevel.TRACE; 
    

    /**
     * Return the maximum length of any tree path selected.
    */
    public static int maxSelectedTreePathLength(JTree tree) {
        int max = 0;
        if (tree != null) {
            if (tree.getSelectionCount() > 0) {
                TreePath[] treePath = tree.getSelectionPaths();
                for (int i = 0; i < treePath.length; i++) {
                    int cur = treePath[i].getPathCount();
                    if (cur > max) {
                        max = cur;
                    }
                }
            }
        }
        return max;
    }


    /**
     * Expand all rows of a JTree.  (If there is a method to do this in JTree I
     * cannot find it.  BWP)
     */
    public final static void expandAll(JTree tree) {
    	TreePath[] tp = null;
    	try {
    		tp = StaticTreeUtilities.allTreePaths(tree.getModel(), false);
    	} catch (Exception ex) {
    		//Cannot occur-- not populating node.
    	}
    	for (int i = 0; i < tp.length; i++) {
    		tree.expandPath(tp[i]);
    	}
	}

    /**
     * Return all of the descendants of a javax.swing.tree.TreeNode as a List,
     * optionally including the starting node.
     */
    public static java.util.List /*<javax.swing.tree.TreeNode>*/ descendantsOfNode(
            javax.swing.tree.TreeNode startingNode, boolean includeStartingNode) {
        ArrayList list = new ArrayList();
        if (includeStartingNode) {
            list.add(startingNode);
        }
        if (startingNode.getChildCount() > 0) {
            javax.swing.tree.TreeNode curNode = startingNode.getChildAt(0);
            while (curNode != startingNode) {
                list.add(curNode);
                if (curNode.getChildCount() > 0) {
                    curNode = curNode.getChildAt(0);
                } else {
                    boolean nextChildFound = false;
                    while ((curNode != startingNode) && (!nextChildFound)) {
                        javax.swing.tree.TreeNode parent = curNode.getParent();
                        int prevIndex = parent.getIndex(curNode);
                        if (prevIndex < parent.getChildCount() - 1) {
                            curNode = parent.getChildAt(prevIndex + 1);
                            nextChildFound = true;
                        } else {
                            curNode = parent;
                        }
                    }
                }
            }
        }
        return list;
    }

    /**
     * Return all of the descendants of a
     * DefaultTreeNode as a List,
     * optionally including the starting node.
     */
    public static java.util.List
            /*<DefaultTreeNode>*/
            descendantsOfNode(
            DefaultTreeNode startingNode,
            boolean includeStartingNode) {
        ArrayList list = new ArrayList();
        if (includeStartingNode) {
            list.add(startingNode);
        }
        if (startingNode.getChildCount() > 0) {
            DefaultTreeNode curNode =
                    startingNode.getChild(0);
            while (curNode != startingNode) {
                list.add(curNode);
                if (curNode.getChildCount() > 0) {
                    curNode = curNode.getChild(0);
                } else {
                    boolean nextChildFound = false;
                    while ((curNode != startingNode) && (!nextChildFound)) {
                        DefaultTreeNode parent =
                                curNode.getParent();
                        int prevIndex = parent.getChildIndex(curNode);
                        if (prevIndex < parent.getChildCount() - 1) {
                            curNode = parent.getChild(prevIndex + 1);
                            nextChildFound = true;
                        } else {
                            curNode = parent;
                        }
                    }
                }
            }
        }
        return list;
    }

    /**
     * Return all of the descendants of a com.metamatrix.common.tree.TreeNode as a List,
     * optionally including the starting node.
     */
    public static java.util.List /*<com.metamatrix.common.tree.TreeNode>*/ descendantsOfNode(
            com.metamatrix.common.tree.TreeNode startingNode, boolean includeStartingNode,
            TreeView treeView) {
        ArrayList list = new ArrayList();
        if (includeStartingNode) {
            list.add(startingNode);
        }
        int startingNodeChildCount = treeView.getChildren(startingNode).size();
        if (startingNodeChildCount > 0) {
            com.metamatrix.common.tree.TreeNode curNode =
                    (com.metamatrix.common.tree.TreeNode)treeView.getChildren(
                    startingNode).get(0);
            while (curNode != startingNode) {
                list.add(curNode);
                int curNodeChildCount = treeView.getChildren(curNode).size();
                if (curNodeChildCount > 0) {
                    curNode = (com.metamatrix.common.tree.TreeNode)
                            treeView.getChildren(curNode).get(0);
                } else {
                    boolean nextChildFound = false;
                    while ((curNode != startingNode) && (!nextChildFound)) {
                        com.metamatrix.common.tree.TreeNode parent = treeView.getParent(curNode);
                        java.util.List parentsChildren = treeView.getChildren(
                                parent);
                        int prevIndex = parentsChildren.indexOf(curNode);
                        if (prevIndex < parentsChildren.size() - 1) {
                            curNode = (com.metamatrix.common.tree.TreeNode)
                                    parentsChildren.get(prevIndex + 1);
                            nextChildFound = true;
                        } else {
                            curNode = parent;
                        }
                    }
                }
            }
        }
        return list;
    }

    /**
     * Return all of the descendants of a javax.swing.tree.TreeNode as a List,
     * never including the starting node.
     */
    public static java.util.List /*of javax.swing.tree.TreeNode*/ descendantsOfNode(
            javax.swing.tree.TreeNode startingNode) {
        return descendantsOfNode(startingNode, false);
    }

    /**
     * Collapse an entire tree.
     */
    public static void collapse(JTree tree) {
        boolean foundCollapsibleRow = true;
        while (foundCollapsibleRow) {
            foundCollapsibleRow = false;
            int numRows = tree.getRowCount();
            for (int curRow = numRows - 1; curRow >= 0; curRow--) {
                if (!tree.isCollapsed(curRow)) {
                    tree.collapseRow(curRow);
                    foundCollapsibleRow = true;
                }
            }
        }
    }

    /**
     * For debug.  Represent a tree as a string, with children indented.
     * Assumes all nodes are DefaultMutableTreeNodes.
     */
    public static String nodesString(DefaultTreeModel model) {
        String str = "";
        DefaultMutableTreeNode root = (DefaultMutableTreeNode)model.getRoot();
        DefaultMutableTreeNode curNode = root;
        int level = 0;
        boolean done = false;
        while (!done) {
            if (curNode != root) {
                str += "\n";
            }
            int numBlanks = 4 * level;
            for (int i = 0; i < numBlanks; i++) {
                str += " ";
            }
            str += curNode.getClass().getName() + ":  " + curNode.toString();
            if (curNode.getChildCount() > 0) {
                curNode = (DefaultMutableTreeNode)curNode.getChildAt(0);
                level++;
            } else {
                boolean nextNodeFound = false;
                while ((!done) && (!nextNodeFound)) {
                    if (curNode == root) {
                        done = true;
                    } else {
                        DefaultMutableTreeNode parent =
                                (DefaultMutableTreeNode)curNode.getParent();
                        int childIndex = parent.getIndex(curNode);
                        if (childIndex < parent.getChildCount() - 1) {
                            curNode = (DefaultMutableTreeNode)parent.getChildAt(childIndex + 1);
                            nextNodeFound = true;
                        } else {
                            curNode = parent;
                            level--;
                        }
                    }
                }
            }
        }
        return str;
    }

    public static String nodesString(JTree tree) {
        String str = null;
        TreeModel tm = tree.getModel();
        if (tm instanceof DefaultTreeModel) {
            DefaultTreeModel dtm = (DefaultTreeModel)tm;
            str = StaticTreeUtilities.nodesString(dtm);
        }
        return str;
    }
    
    /**
     * For debug.  toString() implementation for a TreePath, given the TreePath;
     */
    public static String treePathToString(TreePath tp, boolean classNamesOnly) {
        String outStr = "";
        if (tp == null) {
            outStr = "null";
        } else {
            Object[] node = tp.getPath();
            for (int i = 0; i < node.length; i++) {
                String nodeStr = null;
                if (classNamesOnly) {
                    if (node[i] == null) {
                        nodeStr = "node null";
                    } else {
                        if (node[i] instanceof DefaultMutableTreeNode) {
                            Object userObj = ((DefaultMutableTreeNode)
                                    node[i]).getUserObject();
                            if (userObj == null) {
                                nodeStr = "userObject null";
                            } else {
                                nodeStr = userObj.getClass().getName() +
                                        tabbedPaneString(userObj);
                            }
                        } else {
                            nodeStr = "node not DefaultMutableTreeNode";
                        }
                    }
                } else {
                    nodeStr = node[i].toString();
                }
                if (nodeStr == null) {
                    nodeStr = "null";
                }
                if (i > 0) {
                    outStr += "->";
                }
                outStr += nodeStr;
            }
        }
        return outStr;
    }

    public static String treePathToString(TreePath tp) {
        return treePathToString(tp, false);
    }

    /**
     * For debug.  toString() implementation for a TreePath, given the path's
     * last node.
     */
    public static String treePathToString(javax.swing.tree.TreeNode node,
            boolean classNamesOnly) {
        javax.swing.tree.TreeNode curNode = node;
        Vector nodes = new Vector();
        nodes.add(curNode);
        while (curNode.getParent() != null) {
            curNode = curNode.getParent();
            nodes.add(curNode);
        }
        String outStr = "";
        for (int i = nodes.size() - 1; i >= 0; i--) {
            String nodeStr;
            if (classNamesOnly) {
                if (node instanceof DefaultMutableTreeNode) {
                    Object userObj = ((DefaultMutableTreeNode)node).getUserObject();
                    if (userObj == null) {
                        nodeStr = "userObject null";
                    } else {
                        nodeStr = userObj.getClass().getName();
                    }
                } else {
                    nodeStr = "node not DefaultMutableTreeNode";
                }
            } else {
                nodeStr = nodes.get(i).toString();
                if (nodeStr == null) {
                    nodeStr = "null";
                }
            }
            if (i < nodes.size() - 1) {
                outStr += "->";
            }
            outStr += nodeStr;
        }
        return outStr;
    }

    public static String treePathToString(javax.swing.tree.TreeNode node) {
        return treePathToString(node, false);
    }

    /**
     * Return an array of all tree paths in a tree model.
     */
    public static TreePath[] allTreePaths(TreeModel tm, boolean populate,
            com.metamatrix.common.tree.TreeView treeView) throws Exception {
        boolean javaTreeNode = true;
        Object rootObj = tm.getRoot();
        if (rootObj instanceof javax.swing.tree.TreeNode) {
            javaTreeNode = true;
        } else if (rootObj instanceof com.metamatrix.common.tree.TreeNode) {
            javaTreeNode = false;
        } else {
            throw new Exception("Illegal root node object type: " + rootObj.getClass().getName());
        }
        //Do we need to populate the tree first?
        if (populate) {
            if (javaTreeNode) {
                StaticTreeUtilities.populateTree((javax.swing.tree.TreeNode)rootObj);
            } else {
                StaticTreeUtilities.populateTree((com.metamatrix.common.tree.TreeNode)rootObj,
                        treeView);
            }
        }
        //Get list of all nodes in the tree
        java.util.List nodes;
        if (javaTreeNode) {
            nodes = StaticTreeUtilities.descendantsOfNode((javax.swing.tree.TreeNode)
                    tm.getRoot(), true);
        } else if ((tm.getRoot() instanceof 
        		DefaultTreeNode) &&
        		(treeView == null)) {
			nodes = StaticTreeUtilities.descendantsOfNode(
        			(DefaultTreeNode)
        			tm.getRoot(), true);
		} else {
			nodes = StaticTreeUtilities.descendantsOfNode((com.metamatrix.common.tree.TreeNode)
                    tm.getRoot(), true, treeView);
		}

        //Accumulate list containing path to each node
        java.util.List /*of TreePath*/ paths = new ArrayList();
        Iterator it = nodes.iterator();
        while (it.hasNext()) {
            java.util.List curPath;
            if (javaTreeNode) {
                javax.swing.tree.TreeNode curNode = (javax.swing.tree.TreeNode)it.next();

                //Accumulate list that will contain the path to this node, initially in reverse
                //order
                curPath = new ArrayList();
                curPath.add(curNode);
                while (curNode.getParent() != null) {
                    curNode = curNode.getParent();
                    curPath.add(curNode);
                }
        	} else if ((tm.getRoot() instanceof 
        			DefaultTreeNode) &&
        			(treeView == null)) {
                DefaultTreeNode curNode =
                        (DefaultTreeNode)it.next();
                curPath = new ArrayList();
                curPath.add(curNode);
				while (curNode.getParent() != null) {
					curNode = curNode.getParent();
					curPath.add(curNode);
                }
			} else {
                com.metamatrix.common.tree.TreeNode curNode =
                        (com.metamatrix.common.tree.TreeNode)it.next();
                curPath = new ArrayList();
                curPath.add(curNode);
                while (treeView.getParent(curNode) != null) {
                    curNode = treeView.getParent(curNode);
                    curPath.add(curNode);
                }
            }

            //Invert the order of the nodes in curPath and put them into an Object array, to
            //become a TreePath.
            Object[] objArray = new Object[curPath.size()];
            for (int i = 0; i < objArray.length; i++) {
                objArray[objArray.length - 1 - i] = curPath.get(i);
            }
            TreePath tp = new TreePath(objArray);
            paths.add(tp);
        }

        //Convert list of tree paths into an array
        TreePath[] tp = new TreePath[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
            tp[i] = (TreePath)paths.get(i);
        }
		return tp;
    }

    public static TreePath[] allTreePaths(TreeModel tm,
            com.metamatrix.common.tree.TreeView treeView) throws Exception {
        return allTreePaths(tm, false, treeView);
    }

    public static TreePath[] allTreePaths(TreeModel tm,
            boolean populate) throws Exception {
        return allTreePaths(tm, populate, null);
    }

	public static TreePath[] allTreePathsToLeafNodes(TreeModel tm) {
		TreePath[] allPaths = null;
		try {
			allPaths = allTreePaths(tm, false, null);
		} catch (Exception ex) {
			//Cannot occur-- not populating tree
		}
		java.util.List /*<TreePath>*/ leafNodePaths = new ArrayList();
		for (int i = 0; i < allPaths.length; i++) {
			Object lastPathComponent = allPaths[i].getLastPathComponent();
			if (lastPathComponent instanceof DefaultMutableTreeNode) {
				DefaultMutableTreeNode lastNode = 
						(DefaultMutableTreeNode)lastPathComponent;
				if (lastNode.isLeaf()) {
					leafNodePaths.add(allPaths[i]);
				}
			} else if (lastPathComponent instanceof 
					DefaultTreeNode) {
				DefaultTreeNode lastNode =
						(DefaultTreeNode)
						lastPathComponent;
				if (lastNode.getChildCount() == 0) {
					leafNodePaths.add(allPaths[i]);
				}
			}
		}
		TreePath[] leafNodePathsArray = new TreePath[leafNodePaths.size()];
		Iterator it = leafNodePaths.iterator();
		for (int i = 0; it.hasNext(); i++) {
			leafNodePathsArray[i] = (TreePath)it.next();
		}
		return leafNodePathsArray;
	}
	
    /**
     * Returns a String representing all of the tree paths in a tree, also showing
     * whether or not each tree path is expanded.
     */
    public static String allTreePathsToString(JTree tree,
            com.metamatrix.common.tree.TreeView treeView) {
        TreePath[] treePaths = null;
        try {
            treePaths = allTreePaths(tree.getModel(), false, treeView);
        } catch (Exception e) {
            //No exception since we are not populating
        }
        String str = "All tree paths:\n";
        for (int i = 0; i < treePaths.length; i++) {
            String treePathString = treePathToString(treePaths[i]);
            str += treePathString;
            if (tree.isExpanded(treePaths[i])) {
                str += " (exp.)";
            } else {
                str += " (not exp.)";
            }
            str += "\n";
        }
        return str;
    }

    /**
     * Returns a String representing all of the tree paths in a tree, also showing
     * whether or not each tree path is expanded.
     */
    public static String allTreePathsToString(TreeModel model,
            boolean classNamesOnly, com.metamatrix.common.tree.TreeView treeView) {
        TreePath[] treePaths = null;
        try {
            treePaths = allTreePaths(model, false, treeView);
        } catch (Exception e) {
            //No exception since we are not populating
        }
        String str = "All tree paths:\n";
        for (int i = 0; i < treePaths.length; i++) {
            String treePathString = treePathToString(treePaths[i], classNamesOnly);
            str += treePathString;
            str += "\n";
        }
        return str;
    }

    public static String allTreePathsToString(TreeModel model) {
        return allTreePathsToString(model, false, null);
    }

    /**
     * Return all tree paths where the last node is a given node.  This will return
     * those tree paths whose last node is a DefaultMutableTreeNode having a user
     * object which matches the given object, using the equals() method.  Does NOT
     * populate any unpopulated LazyBranchNodes.
     */
    public static Collection /*<TreePath>*/ allTreePathsToNode(Object nodeUserObject,
            TreeModel model, com.metamatrix.common.tree.TreeView treeView) {
        Collection paths = new ArrayList();
        TreePath[] tp = null;
        try {
            tp = allTreePaths(model, false, treeView);
        } catch (Exception e) {
            //Should not happen, since not populating
        }
        for (int i = 0; i < tp.length; i++) {
            Object last = tp[i].getLastPathComponent();
            if (last instanceof DefaultMutableTreeNode) {
                DefaultMutableTreeNode lastNode = (DefaultMutableTreeNode)last;
                if ((lastNode.getUserObject() != null) &&
                        lastNode.getUserObject().equals(nodeUserObject)) {
                    paths.add(tp[i]);
                }
            }
        }
        return paths;
    }

    public static Collection /*<TreePath>*/ allTreePathsToNode(Object nodeUserObject,
            TreeModel model) {
        return allTreePathsToNode(nodeUserObject, model, null);
    }

    /**
     * Return the index of a given name in a tree path, using the toString() method of each
     * node.  Return -1 if not found.
     */
    public static int indexInTreePath(String name, TreePath tp) {
        int index = -1;
        if (name != null) {
            Object[] nodes = tp.getPath();
            int loc = 0;
            while ((index == -1) && (loc < nodes.length)) {
                DefaultMutableTreeNode tn = (DefaultMutableTreeNode)nodes[loc];
                String str = tn.toString();
                if (str == null) {
                    str = "";
                }
                if ((str.length() > 0) && str.equals(name)) {
                    index = loc;
                } else {
                    loc++;
                }
            }
        }
        return index;
    }

    /**
     * Given a root node, this method populates any nodes in a tree that are LazyBranchNodes.
     * Unfortunately, this may be required so that it can be known whether or not
     * a child node is eligible to be added.  If it is an ancestor node of the group then
     * it is not eligible, and the tree must be populated to find out.
     */
    public static void populateTree(javax.swing.tree.TreeNode root) throws Exception {
        javax.swing.tree.TreeNode curNode = root;
        boolean done = false;
        while (!done) {
            if (curNode instanceof LazyBranchNode) {
                LazyBranchNode lbn = (LazyBranchNode)curNode;
                if (!lbn.isPopulated()) {
                    lbn.populate();
                }
            }
            if (curNode.getChildCount() > 0) {
                curNode = curNode.getChildAt(0);
            } else {
                boolean nextChildFound = false;
                while ((!done) && (!nextChildFound)) {
                    if (curNode == root) {
                        done = true;
                    } else {
                        javax.swing.tree.TreeNode parent = curNode.getParent();
                        int prevIndex = parent.getIndex(curNode);
                        if (prevIndex < parent.getChildCount() - 1) {
                            curNode = parent.getChildAt(prevIndex + 1);
                            nextChildFound = true;
                        } else {
                            curNode = parent;
                        }
                    }
                }
            }
        }
    }

    /**
     * Given a com.metamatrix.common.tree.TreeNode as a root starting point,
     * not necessarily a real root, populate all of its descendant nodes.  This
     * is done by repetitively calling getChildren();
     */
    public static void populateTree(com.metamatrix.common.tree.TreeNode root,
            TreeView treeView) {
        com.metamatrix.common.tree.TreeNode curNode = root;
        boolean done = false;
        while (!done) {
            java.util.List /*<com...TreeNode>*/ children = treeView.getChildren(curNode);
            int numChildren = children.size();
            if (numChildren > 0) {
                curNode = (com.metamatrix.common.tree.TreeNode)children.get(0);
            } else {
                boolean nextNodeFound = false;
                while ((!nextNodeFound) && (!done)) {
                    if (curNode == root) {
                        done = true;
                    } else {
                        com.metamatrix.common.tree.TreeNode parent =
                                treeView.getParent(curNode);
                        java.util.List /*<com...TreeNode>*/ parentsChildren =
                                treeView.getChildren(parent);
                        int index = parentsChildren.indexOf(curNode);
                        if (index < parentsChildren.size() - 1) {
                            curNode = (com.metamatrix.common.tree.TreeNode)
                                    parentsChildren.get(index + 1);
                            nextNodeFound = true;
                        } else {
                            curNode = parent;
                        }
                    }
                }
            }
        }
    }

    public static TreePathExpansion[] expansionState(JTree tree,
            com.metamatrix.common.tree.TreeView treeView, boolean populate)
            throws Exception {
        TreePath[] tp = allTreePaths(tree.getModel(), populate, treeView);
        TreePathExpansion[] tpe = new TreePathExpansion[tp.length];
        for (int i = 0; i < tpe.length; i++) {
            boolean expanded = false;
            boolean hasBeenSet = false;
            Object lastNode = tp[i].getLastPathComponent();
            if (lastNode != null) {
                boolean lastNodeIsLeaf;
                if (treeView == null) {
                    javax.swing.tree.TreeNode tn = (javax.swing.tree.TreeNode)lastNode;
                    lastNodeIsLeaf = (tn.getChildCount() == 0);
                } else {
                    com.metamatrix.common.tree.TreeNode tn =
                            (com.metamatrix.common.tree.TreeNode)lastNode;
                    lastNodeIsLeaf = (treeView.getChildren(tn).size() == 0);
                }
                if (lastNodeIsLeaf) {
                    //Last node is a leaf.  We will return true if tree path excluding leaf is
                    //expanded.
                    Object[] parentPathNode = new Object[tp[i].getPathCount() - 1];
                    for (int j = 0; j < parentPathNode.length; j++) {
                        parentPathNode[j] = tp[i].getPathComponent(j);
                    }
                    TreePath parentPath = new TreePath(parentPathNode);
                    expanded = tree.isExpanded(parentPath);
                    hasBeenSet = true;
                }
            }
            if (!hasBeenSet) {
                expanded = tree.isExpanded(tp[i]);
            }
            tpe[i] = new TreePathExpansion(tp[i], expanded);
        }
        return tpe;
    }

    public static TreePathExpansion[] expansionState(JTree tree)
            throws Exception {
        return expansionState(tree, null, true);
    }

    public static TreePathExpansion[] expansionState(JTree tree, boolean populate) 
            throws Exception {
        return expansionState(tree, null, populate);
    }
    
    public static void restoreExpansionState(JTree tree,
            TreePathExpansion[] savedPathState, boolean expandNewPaths,
            Collection /*<TreeSelectionListener>*/ selectionListeners,
            Collection /*<TreeWillExpandListener>*/ willExpandListeners,
            Collection /*<TreeExpansionListener>*/ expansionListeners,
            com.metamatrix.common.tree.TreeView treeView)
            throws Exception {
        //Save the tree's current selections
        TreePath[] selectedPaths = tree.getSelectionPaths();
        //Remove any listed listeners.  Will reinstate them after expansion.
        if ((selectionListeners != null) && (selectionListeners.size() > 0)) {
            Iterator it = selectionListeners.iterator();
            while (it.hasNext()) {
                TreeSelectionListener listener = (TreeSelectionListener)it.next();
                tree.removeTreeSelectionListener(listener);
            }
        }
        if ((willExpandListeners != null) && (willExpandListeners.size() > 0)) {
            Iterator it = willExpandListeners.iterator();
            while (it.hasNext()) {
                TreeWillExpandListener listener = (TreeWillExpandListener)it.next();
                tree.removeTreeWillExpandListener(listener);
            }
        }
        if ((expansionListeners != null) && (expansionListeners.size() > 0)) {
            Iterator it = expansionListeners.iterator();
            while (it.hasNext()) {
                TreeExpansionListener listener = (TreeExpansionListener)it.next();
                tree.removeTreeExpansionListener(listener);
            }
        }
        
        //Get array of tree paths
        TreePath[] tp = allTreePaths(tree.getModel(), true, treeView);

        //Start out tree in collapsed state, then expand any paths that were previously
        //expanded.  Also optionally expand any new paths.
        collapse(tree);
        for (int i = 0; i < tp.length; i++) {
            //Attempt to find this path among the saved paths.
            boolean found = false;
            int j = 0;
            while ((!found) && (j < savedPathState.length)) {
                if (treePathsEqual(tp[i], savedPathState[j].getTreePath())) {
                    found = true;
                } else {
                    j++;
                }
            }
            boolean expanding = false;
            if (found) {
                if (savedPathState[j].isExpanded()) {
                    expanding = true;
                }
            } else {
                if (expandNewPaths) {
                    expanding = true;
                }
            }
            if (expanding) {
                tree.expandPath(tp[i]);
            }
        }

        //Now re-select any selected paths
        try {
            tree.setSelectionPaths(selectedPaths);
        } catch (Exception e) {
        }

        //Now reinstate any listeners that we removed
        if ((selectionListeners != null) && (selectionListeners.size() > 0)) {
            Iterator it = selectionListeners.iterator();
            while (it.hasNext()) {
                TreeSelectionListener listener = (TreeSelectionListener)it.next();
                tree.addTreeSelectionListener(listener);
            }
        }
        if ((willExpandListeners != null) && (willExpandListeners.size() > 0)) {
            Iterator it = willExpandListeners.iterator();
            while (it.hasNext()) {
                TreeWillExpandListener listener = (TreeWillExpandListener)it.next();
                tree.addTreeWillExpandListener(listener);
            }
        }
        if ((expansionListeners != null) && (expansionListeners.size() > 0)) {
            Iterator it = expansionListeners.iterator();
            while (it.hasNext()) {
                TreeExpansionListener listener = (TreeExpansionListener)it.next();
                tree.addTreeExpansionListener(listener);
            }
        }
    }

    public static void restoreExpansionState(JTree tree,
            TreePathExpansion[] savedPathState, boolean expandNewPaths,
            Collection /*<TreeSelectionListener>*/ selectionListeners,
            Collection /*<TreeWillExpandListener>*/ willExpandListeners,
            Collection /*<TreeExpansionListener>*/ expansionListeners)
            throws Exception {
        restoreExpansionState(tree, savedPathState, expandNewPaths,
                selectionListeners, willExpandListeners, expansionListeners,
                null);
    }

    public static void restoreExpansionState(JTree tree,
            TreePathExpansion[] savedPathState, boolean expandNewPaths,
            TreeView treeView)
            throws Exception {
        restoreExpansionState(tree, savedPathState, expandNewPaths,
                null, null, null, treeView);
    }

    public static void restoreExpansionState(JTree tree,
            TreePathExpansion[] savedPathState, boolean expandNewPaths)
            throws Exception {
        restoreExpansionState(tree, savedPathState, expandNewPaths,
                null, null, null, null);
    }

    public static TreePath[] allExpandedPaths(JTree tree,
            com.metamatrix.common.tree.TreeView treeView) {
        TreePath[] allPaths = null;
        try {
            allPaths = StaticTreeUtilities.allTreePaths(tree.getModel(), false,
                    treeView);
        } catch (Exception e) {
            //Will not throw exception since not populating
        }
        Collection /*<TreePath>*/ expandedPaths = new ArrayList();
        for (int i = 0; i < allPaths.length; i++) {
            if (tree.isExpanded(allPaths[i])) {
                expandedPaths.add(allPaths[i]);
            }
        }
        TreePath[] array = new TreePath[expandedPaths.size()];
        Iterator it = expandedPaths.iterator();
        for (int i = 0; it.hasNext(); i++) {
            array[i] = (TreePath)it.next();
        }
        return array;
    }

    /**
     * Compare two tree paths using toString()
     */
    public static boolean treePathsEqual(TreePath tp1, TreePath tp2) {
        boolean equal = false;
        if ((tp1 == null) && (tp2 == null)) {
            equal = true;
        } else {
            if ((tp1 != null) && (tp2 != null)) {
                if (tp1.getPathCount() == tp2.getPathCount()) {
                    boolean mismatchFound = false;
                    int i = 0;
                    while ((i < tp1.getPathCount()) && (!mismatchFound)) {
                        String str1 = tp1.getPathComponent(i).toString();
                        String str2 = tp2.getPathComponent(i).toString();
                        boolean itemsMatch = false;
                        if ((str1 == null) && (str2 == null)) {
                            itemsMatch = true;
                        } else {
                            itemsMatch = str1.equals(str2);
                        }
                        if (itemsMatch) {
                            i++;
                        } else {
                            mismatchFound = true;
                        }
                    }
                    equal = !mismatchFound;
                }
            }
        }
        return equal;
    }

    /**
     * Returns the first node found whose user object equals the given object.
     * This method assumes that all nodes in the given tree are
     * DefaultMutableTreeNodes, and may throw an exception if this is not true.
     */
    public static DefaultMutableTreeNode nodeWithObjectOf(TreeModel tree, Object userObj) {
        DefaultMutableTreeNode theNode = null;
        DefaultMutableTreeNode root = (DefaultMutableTreeNode)tree.getRoot();
        List /*of DefaultMutableTreeNode*/ nodes = descendantsOfNode(root, true);
        Iterator it = nodes.iterator();
        while ((theNode == null) && it.hasNext()) {
            DefaultMutableTreeNode curNode = (DefaultMutableTreeNode)it.next();
            if (curNode.getUserObject() != null) {
                if (curNode.getUserObject().equals(userObj)) {
                    theNode = curNode;
                }
            }
        }
        return theNode;
    }

    /**
     * Deselect all selected tree paths in a JTree.
     */
    public static void deselectAll(JTree tree) {
        TreePath[] tp = tree.getSelectionPaths();
        tree.removeSelectionPaths(tp);
    }

    /**
     * Remove all occurrences of a node from a tree model.  Assumes the model
     * contains only DefaultMutableTreeNode objects, and uses the "equals"
     * method to compare the user object of each node to the given
     * parameter.  Returns number of occurrences of the node that were removed.
     */
    public static int removeNode(Object node, TreeModel model) {
        DefaultMutableTreeNode root = (DefaultMutableTreeNode)model.getRoot();
        int removalCount = 0;
        if (root.getChildCount() > 0) {
            boolean done = false;
            DefaultMutableTreeNode curNode = (DefaultMutableTreeNode)root.getChildAt(0);
            DefaultMutableTreeNode curParent = root;
            while (!done) {
                if (curNode.getUserObject().equals(node)) {
                    curParent.remove(curNode);
                    removalCount++;
                    curNode = curParent;
                    curParent = (DefaultMutableTreeNode)curNode.getParent();
                } else {
                    if (curNode.getChildCount() > 0) {
                        curParent = curNode;
                        curNode = (DefaultMutableTreeNode)curNode.getChildAt(0);
                    } else {
                        boolean nextNodeFound = false;
                        while ((!nextNodeFound) && (!done)) {
                            int indexOfNodeToParent = curParent.getIndex(curNode);
                            if (indexOfNodeToParent < curParent.getChildCount() - 1) {
                                curNode = (DefaultMutableTreeNode)curParent.getChildAt(
                                        indexOfNodeToParent + 1);
                                nextNodeFound = true;
                            } else {
                                curNode = curParent;
                                if (curNode == root) {
                                    done = true;
                                } else {
                                    curParent = (DefaultMutableTreeNode)curNode.getParent();
                                }
                            }
                        }
                    }
                }
            }
        }
        return removalCount;
    }

    /**
     * Return the child index in a DefaultMutableTreeNode of a child whose user object
     * equals the given object.  Returns -1 if not found.
     */
    public static int indexOfUserObject(DefaultMutableTreeNode node,
            Object userObject) {
        int childCount = node.getChildCount();
        boolean found = false;
        int matchLoc = -1;
        int loc = 0;
        while ((loc < childCount) && (!found)) {
            javax.swing.tree.TreeNode tn = node.getChildAt(loc);
            if (tn instanceof DefaultMutableTreeNode) {
                DefaultMutableTreeNode dtn = (DefaultMutableTreeNode)tn;
                if ((dtn.getUserObject() != null) && dtn.getUserObject().equals(
                        userObject)) {
                    found = true;
                    matchLoc = loc;
                }
            }
            loc++;
        }
        return matchLoc;
    }

    /**
     * Return a String array representing the toString() results of each element in a
     * TreePath.
     */
    public static String[] treePathToStrings(TreePath tp) {
        Object[] pathNodes = tp.getPath();
        String[] s = new String[pathNodes.length];
        for (int i = 0; i < pathNodes.length; i++) {
            s[i] = pathNodes[i].toString();
        }
        return s;
    }

    /**
     * Return the node at the end of a "path" consisting an array of Strings
     * representing the toString() results of a corresponding tree node.
     */
    public static DefaultMutableTreeNode nodeAtToStringPath(TreeModel tm, String[] tsp) {
        DefaultMutableTreeNode lastNode = null;
        DefaultMutableTreeNode currentNode = null;
        boolean noMatch = false;
        int i = 0;
        while ((i < tsp.length) && (!noMatch)) {
            List /*<DefaultMutableTreeNode>*/ possibleNodes = new ArrayList();
            if (i == 0) {
                possibleNodes.add(tm.getRoot());
            } else {
                Enumeration enumeration = currentNode.children();
                while (enumeration.hasMoreElements()) {
                    possibleNodes.add(enumeration.nextElement());
                }
            }
            boolean match = false;
            Iterator it = possibleNodes.iterator();
            while ((!match) && it.hasNext()) {
                DefaultMutableTreeNode thisNode = (DefaultMutableTreeNode)it.next();
                if (thisNode.toString().equals(tsp[i])) {
                    match = true;
                    currentNode = thisNode;
                }
            }
            if (!match) {
                noMatch = true;
            } else {
                i++;
            }
        }
        if (!noMatch) {
            lastNode = currentNode;
        }
        return lastNode;
    }

    /**
     * Method inserted specifically to help solve a User tab painting problem.
     * Duplicates of nodes are sporadically being seen
     * in the User tab's trees.  The goal is to call this method when painting,
     * to see if the duplicates are also in the tree model.  This method will check the
     * tree's model, and will log a message to System.err if it finds a duplicate
     * of any node.
     */
    public static void userTabPaintProblemDuplicateNodeCheck(JTree tree) {

        /*
         * jh Defect 22436 - rewrote this method to only do its work when
         *                   msg level = trace.
         */
        
        if ( !LogManager.isMessageToBeRecorded( StaticTreeUtilities.CONTEXT_FOR_DUPE_CHECKING,
                                                StaticTreeUtilities.MSGLEVEL_FOR_DUPE_CHECKING ) ) {
//            System.out.println("[userTabPaintProblemDuplicateNodeCheck] Will NOT check dupes in tree");
            return;
        } 
        
//        System.out.println("[userTabPaintProblemDuplicateNodeCheck] WILL check dupes in tree");

        java.util.List nodesList = StaticTreeUtilities.descendantsOfNode(
                (javax.swing.tree.TreeNode)tree.getModel().getRoot(), true);
        int listLen = nodesList.size();
        for (int i = 0; i < listLen; i++) {
            String str1 = nodesList.get(i).toString();
            for (int j = i + 1; j < listLen; j++) {
                String str2 = nodesList.get(j).toString();
                if (str1.equals(str2)) {
                    String outStr = StaticTreeUtilities.nodesString(tree);
                    LogManager.log( StaticTreeUtilities.MSGLEVEL_FOR_DUPE_CHECKING,
                                    StaticTreeUtilities.CONTEXT_FOR_DUPE_CHECKING,  
                                    "Duplicate nodes found in StaticTreeUtilities.userTabPaintProblemDuplicateNodeCheck():\n" + outStr);
                    return;
                }
            }
        }
        return;
    }
    
    public static TreePath treePathToNode(JTree tree, Object node) {
        TreePath path = null;
        int numRows = tree.getRowCount();
        int i = 0;
        while ((path == null) && (i < numRows)) {
            TreePath tp = tree.getPathForRow(i);
            if (tp != null) {
                Object lastNode = tp.getLastPathComponent();
                if (lastNode == node) {
                    path = tp;
                }
            }
            if (path == null) {
                i++;
            }
        }
        return path;
    }

    public static TreePath treePathToNodeUsingEquals(JTree tree, Object node) {
        TreePath path = null;
        int numRows = tree.getRowCount();
        int i = 0;
        while ((path == null) && (i < numRows)) {
            TreePath tp = tree.getPathForRow(i);
            if (tp != null) {
                Object lastNode = tp.getLastPathComponent();
                if (lastNode.equals(node)) {
                    path = tp;
                }
            }
            if (path == null) {
                i++;
            }
        }
        return path;
    }

///////////////////////////
//
// Private internal methods
//
///////////////////////////

    private static String tabbedPaneString(Object obj) {
        String str = "";
        if (obj instanceof JTabbedPane) {
            JTabbedPane pane = (JTabbedPane)obj;
            str = "[";
            for (int i = 0; i < pane.getTabCount(); i++) {
                str += pane.getTitleAt(i) + ":";
                str += pane.getComponentAt(i).getClass().getName();
                if (i < pane.getTabCount() - 1) {
                    str += ",";
                }
            }
            str += "]";
        }
        return str;
    }
}
