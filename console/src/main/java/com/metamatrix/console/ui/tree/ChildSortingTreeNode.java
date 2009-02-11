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

package com.metamatrix.console.ui.tree;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.MutableTreeNode;

import com.metamatrix.console.util.StaticTreeSortUtilities;

/**
 * Extension to DefaultMutableTreeNode that implements SortsChildren-- always keeps its children
 * in a sorted order.  This is necessary because the order of child nodes as returned from the
 * server is not consistent, which if allowed is very inconvenient for users.
 */
public class ChildSortingTreeNode extends DefaultMutableTreeNode implements SortsChildren {
    private int sortType = SortsChildren.ALPHABETIC_SORT;
    private DefaultTreeModel model;
        //Unfortunately, must know tree model so we can call model's reload() method whenever
        //sort order changes.  Otherwise, repainting problems occur.
    private boolean usingUserObjectToString = false;

    public ChildSortingTreeNode(DefaultTreeModel mdl) {
        super();
        model = mdl;
    }

    public ChildSortingTreeNode(DefaultTreeModel mdl, Object userObj) {
        super(userObj);
        model = mdl;
    }

    public ChildSortingTreeNode(DefaultTreeModel mdl, Object userObj, boolean childrenFlag) {
        super(userObj, childrenFlag);
        model = mdl;
    }

    public DefaultTreeModel getModel() {
        return model;
    }

    public void setModel(DefaultTreeModel mdl) {
        model = mdl;
    }

    public boolean isUsingUserObjectToString() {
        return usingUserObjectToString;
    }

    public void setUsingUserObjectToString(boolean flag) {
        usingUserObjectToString = flag;
    }

    public void setSortType(int val) {
        sortType = val;
    }

    public int getSortType() {
        return sortType;
    }

    /**
     * Sort the child nodes.
     *
     * @returns true if sort order changed, else false
     */
    public boolean sortChildren() {
        boolean changed = false;
        if (getSortType() != SortsChildren.NO_SORT) {
            //Only bother with sort if two or more children
            if (getChildCount() > 1) {
                if (getSortType() == SortsChildren.NUMERIC_SORT) {
                    changed = sortNumeric();
                } else if (getSortType() == SortsChildren.ALPHABETIC_SORT) {
                    changed = sortAlphabetic();
                }
            }
        }
        return changed;
    }

    /**
     * Sort the child nodes into alphabetic order by value of toString().
     */
    private boolean sortAlphabetic() {
        //In order to make life easier for nodes which for some reason cannot extend
        //DefaultMutableTreeNode but want to keep children in sorted order, relegate the work to
        //a public static method.
        return StaticTreeSortUtilities.sortChildrenAlphabetically(this,
                usingUserObjectToString);
    }

    /**
     * Sort the child nodes into ascending numeric order based on numeric value of toString().
     * Revert to alphabetic sort if any toString() value was not numeric.
     */
    private boolean sortNumeric() {
        //In order to make life easier for nodes which for some reason cannot extend
        //DefaultMutableTreeNode but want to keep children in sorted order, relegate the work to
        //a public static method.
        return StaticTreeSortUtilities.sortChildrenNumerically(this,
                usingUserObjectToString);
    }

    /**
     * Static method to sort all of the nodes of a tree under a given node.  This method is
     * called by SortReadyJTree when the tree model is established, so as to start out in a
     * sorted order which will then be maintained.
     */
    public static void sortTree(MutableTreeNode root) {
        MutableTreeNode curNode= root;
        boolean done = false;
        while (!done) {
            if (curNode instanceof SortsChildren) {
                SortsChildren cn = (SortsChildren)curNode;
                cn.sortChildren();
            }
            if (curNode.getChildCount() > 0) {
                curNode = (MutableTreeNode)curNode.getChildAt(0);
            } else {
                boolean nextNodeFound = false;
                while ((!done) && (!nextNodeFound)) {
                    if (curNode == root) {
                        done = true;
                    } else {
                        MutableTreeNode parent = (MutableTreeNode)curNode.getParent();
                        int nextIndex = parent.getIndex(curNode) + 1;
                        if (nextIndex < parent.getChildCount()) {
                            curNode = (MutableTreeNode)parent.getChildAt(nextIndex);
                            nextNodeFound = true;
                        } else {
                            curNode = parent;
                        }
                    }
                }
            }
        }
    }

//Overridden methods

    public void add(MutableTreeNode child) {
        super.add(child);
        //After adding the child, must now re-sort the children
//        boolean changed = false;
        if (getSortType() != SortsChildren.NO_SORT) {
//            changed = 
            sortChildren();
            if (model != null) {
                model.reload();
            }
        }
    }

    public void insert(MutableTreeNode child, int index) {
        super.insert(child, index);
        //After inserting the child, must re-sort the children (so we effectively ignore the
        //caller-supplied index for the insert).
//        boolean changed = false;
        if (getSortType() != SortsChildren.NO_SORT) {
//            changed = 
            sortChildren();
            if (model != null) {
                model.reload();
            }
        }
    }
}
