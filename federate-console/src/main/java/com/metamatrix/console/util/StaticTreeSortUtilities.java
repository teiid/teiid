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

package com.metamatrix.console.util;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreeNode;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ui.tree.ChildSortingTreeNode;
import com.metamatrix.console.ui.tree.SortsChildren;
import com.metamatrix.console.ui.util.LazyBranchNode;

/**
 *
 */
public class StaticTreeSortUtilities {

    /**
     * Sort the child nodes into alphabetic order by value of toString().  Returns true if
     * sorting changed the order.
     */
    public static boolean sortChildrenAlphabetically(MutableTreeNode node,
            boolean useUserObjectToString) {
        int[] nodesOrder = alphabeticOrder(node, useUserObjectToString);
        return reorderChildren(node, nodesOrder);
    }

    public static boolean sortChildrenAlphabetically(MutableTreeNode node) {
        return sortChildrenAlphabetically(node, false);
    }

    /**
     * Sort the child nodes into ascending numeric order based on numeric value of toString().
     * Revert to alphabetic sort if any toString() value was not numeric.  Returns true if
     * sorting changed the order.
     */
    public static boolean sortChildrenNumerically(MutableTreeNode node,
            boolean useUserObjectToString) {
        boolean changed;
        double[] numberValue = numbersList(node, useUserObjectToString);
        if (numberValue == null) {
            //One or more items were not numeric, so we will revert to an alphabetic sort.
            changed = sortChildrenAlphabetically(node, useUserObjectToString);
        } else {
            int[] nodesOrder = numericOrder(numberValue);
            changed = reorderChildren(node, nodesOrder);
        }
        return changed;
    }

    public static boolean sortChildrenNumerically(MutableTreeNode node) {
        return sortChildrenNumerically(node, false);
    }
    
    /**
     * Return toString() of each child interpreted as a double.  If any non-numeric found,
     * return null.
     */
    private static double[] numbersList(MutableTreeNode node,
            boolean useUserObjectToString) {
        int childCount = node.getChildCount();
        double[] nl = new double[childCount];
        int i = 0;
        while ((nl != null) && (i < childCount)) {
            MutableTreeNode childNode = (MutableTreeNode)node.getChildAt(i);
            String inputString = null;
            boolean useNodeToString = !useUserObjectToString;
            if (!useNodeToString) {
                if (childNode instanceof DefaultMutableTreeNode) {
                    DefaultMutableTreeNode dmtn = (DefaultMutableTreeNode)childNode;
                    Object userObj = dmtn.getUserObject();
                    if (userObj != null) {
                        inputString = userObj.toString().trim();
                    } else {
                        useNodeToString = true;
                    }
                } else {
                    useNodeToString = true;
                }
            }
            if (useNodeToString) {
                inputString = childNode.toString().trim();
            }
            try {
                nl[i] = (new Double(inputString)).doubleValue();
                i++;
            } catch (Exception e) {
                nl = null;
            }
        }
        return nl;
    }

    /**
     * For each index return the current index of the child that should occupy the index to be
     * in sorted order, based on the array of values passed in, which correspond to the numeric
     * value for each child.
     */
    private static int[] numericOrder(double[] value) {
        int[] order = new int[value.length];
        for (int i = 0; i < order.length; i++) {
            order[i] = i;
        }

        //Do bubble sort
        boolean done = false;
        while (!done) {
            done = true;
            for (int i = 0; i < order.length - 1; i++) {
                if (value[i] > value[i + 1]) {
                    done = false;
                    double dtemp = value[i];
                    value[i] = value[i + 1];
                    value[i + 1] = dtemp;
                    int itemp = order[i];
                    order[i] = order[i + 1];
                    order[i + 1] = itemp;
                }
            }
        }
        return order;
    }

    /**
     * For each index return the current index of the child that should occupy the index to be
     * in sorted order.
     */
    private static int[] alphabeticOrder(MutableTreeNode node,
            boolean useUserObjectToString) {
        int childCount = node.getChildCount();
        int[] order = new int[childCount];
        String[] value = new String[childCount];
        for (int i = 0; i < order.length; i++) {
            String val = null;
            MutableTreeNode childNode = (MutableTreeNode)node.getChildAt(i);
            order[i] = i;
            boolean useNodeToString = !useUserObjectToString;
            if (!useNodeToString) {
                if (childNode instanceof DefaultMutableTreeNode) {
                    DefaultMutableTreeNode dmtn =
                            (DefaultMutableTreeNode)childNode;
                    Object userObj = dmtn.getUserObject();
                    if (dmtn != null) {
                        val = userObj.toString().trim();
                    } else {
                        useNodeToString = true;
                    }
                } else {
                    useNodeToString = true;
                }
            }
            if (useNodeToString) {
                val = childNode.toString().trim();
            }
            value[i] = val;
        }

        //Do bubble sort
        boolean done = false;
        while (!done) {
            done = true;
            for (int i = 0; i < childCount - 1; i++) {
                if (value[i].compareToIgnoreCase(value[i + 1]) > 0) {
                    done = false;
                    String stemp = value[i];
                    value[i] = value[i + 1];
                    value[i + 1] = stemp;
                    int itemp = order[i];
                    order[i] = order[i + 1];
                    order[i + 1] = itemp;
                }
            }
        }
        return order;
    }

    /**
     * Re-order the child nodes to the new order, where at each location in newOrder is found
     * the current location of the child node that should be placed in that location.  Returns
     * true if the order has changed, else false.
     */
    private static boolean reorderChildren(MutableTreeNode node, int[] newOrder) {
        boolean changed = false;
        //Did the order even change?
        if (orderChanged(newOrder)) {
            //The order has changed.  Proceed with reordering.
            changed = true;
            Object[] child = new Object[newOrder.length];
            for (int i = 0; i < newOrder.length; i++) {
                child[i] = node.getChildAt(i);
            }
            //Temporarily remove all of the children
            for (int i = newOrder.length - 1; i >= 0; i--) {
                node.remove(i);
            }
            //NOTE-- code inserted here because of strange unreproducible problems
            //being occasionally encountered where a duplicate node is being inserted into
            //the model.  Should somehow the above "for" loop not always be
            //removing all the nodes (don't ask me how), that could explain the
            //symptoms, so let's double check.  BWP  03/12/01
            int childCount = node.getChildCount();
            if (childCount > 0) {
                LogManager.logError(LogContexts.GENERAL, "Loop in " +
                        "StaticTreeSortUtilities..reorderChildren() failed " +
                        "to remove all children.  Would have created " +
                        "duplicate node. Remaining child count is " +
                        childCount);
                while (node.getChildCount() > 0) {
                    node.remove(0);
                }
            }

            //Now re-insert them in order
            int saveSortType = -1;
            ChildSortingTreeNode cstn = null;
            if (node instanceof ChildSortingTreeNode) {
                cstn = (ChildSortingTreeNode)node;
                saveSortType = cstn.getSortType();
                cstn.setSortType(SortsChildren.NO_SORT);
            }
            for (int i = 0; i < child.length; i++) {
                node.insert((MutableTreeNode)child[newOrder[i]], i);
            }
            if (cstn != null) {
                cstn.setSortType(saveSortType);
            }
        }
        return changed;
    }

    private static boolean orderChanged(int[] newOrder) {
        boolean mismatchFound = false;
        int i = 0;
        while ((!mismatchFound) && (i < newOrder.length)) {
            if (newOrder[i] != i) {
                mismatchFound = true;
            }
            i++;
        }
        return mismatchFound;
    }

    public static void sortTreeAlphabetically(TreeNode root,
            boolean useUserObjectToString) throws Exception {
        sortTree(root, false, useUserObjectToString);
    }

    public static void sortTreeNumerically(TreeNode root,
            boolean useUserObjectToString) throws Exception {
        sortTree(root, true, useUserObjectToString);
    }

    private static void sortTree(TreeNode root, boolean tryNumerically,
            boolean useUserObjectToString) throws Exception {
        TreeNode curNode = root;
        boolean done = false;
        while (!done) {
            if (curNode instanceof LazyBranchNode) {
                LazyBranchNode lbn = (LazyBranchNode)curNode;
                if (!lbn.isPopulated()) {
                    lbn.populate();
                }
            }
            if ((curNode.getChildCount() > 0) && (curNode instanceof MutableTreeNode)) {
                MutableTreeNode mtn = (MutableTreeNode)curNode;
                if (tryNumerically) {
                    sortChildrenNumerically(mtn, useUserObjectToString);
                } else {
                    sortChildrenAlphabetically(mtn, useUserObjectToString);
                }
            }
            if (curNode.getChildCount() > 0) {
                curNode = curNode.getChildAt(0);
            } else {
                boolean nextNodeFound = false;
                while ((!done) && (!nextNodeFound)) {
                    if (curNode == root) {
                        done = true;
                    } else {
                        TreeNode parent = curNode.getParent();
                        int prevChildIndex = parent.getIndex(curNode);
                        if (prevChildIndex < parent.getChildCount() - 1) {
                            nextNodeFound = true;
                            curNode = parent.getChildAt(prevChildIndex + 1);
                        } else {
                            curNode = parent;
                        }
                    }
                }
            }
        }
    }
}
