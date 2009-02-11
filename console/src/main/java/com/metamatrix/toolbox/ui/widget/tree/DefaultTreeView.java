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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.tree;

// System imports
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;
import com.metamatrix.common.tree.TreeNodeFilter;
import com.metamatrix.common.tree.TreeNodeIterator;
import com.metamatrix.common.tree.TreeView;

/**
This is the default TreeView used by {@link DefaultTreeModel}.
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class DefaultTreeView
    implements Comparator, TreeView {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    private static final char SEPARATOR_CHARACTER = '/';
    private static final String SEPARATOR_STRING = "" + SEPARATOR_CHARACTER;
    
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private DefaultTreeNode root = null;
    private HashMap childrenMap = null;
    private HashMap parentMap = null;
    private ArrayList rootList = null;
    private transient Object value = null;
    private TreeNodeEditor editor = null;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a DefaultTreeView with an empty {@link DefaultTreeNode} as a root node.
    @since 2.0
    */
    public DefaultTreeView() {
        this(new DefaultTreeNode());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a DefaultTreeView with an empty {@link DefaultTreeNode} as a root node.
    @since 2.0
    */
    public DefaultTreeView(final DefaultTreeNode root) {
        this.root = root;
        initializeDefaultTreeView();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a DefaultTreeView with an empty {@link DefaultTreeNode} as a root node.
    @since 2.0
    */
    public DefaultTreeView(final Object value) {
        this.value = value;
        initializeDefaultTreeView();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean allowsChildren(final TreeNode node) {
        return true;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean allowsChild(TreeNode parent, TreeNode potentialChild) {
        return true;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int compare(final Object firstNode, final Object secondNode) {
        return ((TreeNode)firstNode).compareTo(secondNode);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public UserTransaction createReadTransaction() {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public UserTransaction createWriteTransaction() {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public UserTransaction createWriteTransaction(final Object source) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public String getAbsolutePath(final TreeNode node) {
        return getPath(node);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeNode getChild(final DefaultTreeNode parent, final int index) {
        if (childrenMap != null  &&  childrenMap.containsKey(parent)) {
            final List children = (List)childrenMap.get(parent);
            if (children == null || 
                children.isEmpty() || 
                index >= children.size()) {
                return null;
            }
            return (DefaultTreeNode)children.get(index);
        }
        return parent.getChild(index);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getChildCount(final TreeNode parent) {
        if (childrenMap != null  &&  childrenMap.containsKey(parent)) {
            final List children = (List)childrenMap.get(parent);
            if (children == null) {
                return 0;
            }
            return children.size();
        }
        return ((DefaultTreeNode)parent).getChildCount();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getChildIndex(final DefaultTreeNode parent, final DefaultTreeNode child) {
        if (childrenMap != null  &&  childrenMap.containsKey(parent)) {
            final List children = (List)childrenMap.get(parent);
            if (children == null || children.isEmpty()) {
                return -1;
            }
            return children.indexOf(child);
        }
        return parent.getChildIndex(child);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getChildren(final TreeNode parent) {
        if (childrenMap != null  &&  childrenMap.containsKey(parent)) {
            return (List)childrenMap.get(parent);
        }
        return ((DefaultTreeNode)parent).getChildren();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Iterator iterator() {
        return new TreeNodeIterator(this.getRoot(),this);
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Iterator iterator(TreeNode startingPoint) {
        return new TreeNodeIterator(startingPoint,this);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Comparator getComparator() {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public PropertiedObjectEditor getPropertiedObjectEditor() {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TreeNodeFilter getFilter() {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TreeNode getHome() {
        return getRoot();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TreeNode getParent(final TreeNode child) {
        if (parentMap != null  &&  parentMap.containsKey(child)) {
            return (TreeNode)parentMap.get(child);
        }
        return ((DefaultTreeNode)child).getParent();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public String getPath(final TreeNode node) {
        final StringBuffer buf = new StringBuffer(SEPARATOR_CHARACTER);
        buf.append(node);
        TreeNode parent = getParent(node);
        while (parent != null) {
            buf.insert(0, parent);
            buf.insert(0, SEPARATOR_CHARACTER);
            parent = getParent(node);
        }
        return buf.toString();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getPropertyDefinitions() {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeNode getRoot() {
        return root;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getRoots() {
        if (rootList == null) {
            rootList = new ArrayList();
            rootList.add(getRoot());
        } else {
            rootList.set(0, getRoot());
        }
        return Collections.unmodifiableList(rootList);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Not implemented.
    @since 2.0
    */
    public String getSeparator() {
        return SEPARATOR_STRING;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public char getSeparatorChar() {
        return SEPARATOR_CHARACTER;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TreeNodeEditor getTreeNodeEditor() {
        if (editor == null) {
            editor = new DefaultTreeNodeEditor(root, parentMap, childrenMap);
        }
        return editor;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeDefaultTreeView() {
        if (root == null) {
            if (value == null) {
                root = new DefaultTreeNode();
            } else {
                if (value instanceof DefaultTreeNode) {
                    root = (DefaultTreeNode)value;
                } else {
                    root = new DefaultTreeNode(value.toString());
                }
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isAncestorOf(final TreeNode ancestor, TreeNode descendent) {
        TreeNode parent;
        do {
            parent = getParent(descendent);
            if (parent == ancestor) {
                return true;
            }
            descendent = parent;
        } while (parent != null);
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isHidden(final TreeNode node) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isMarked(final TreeNode node) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isParentOf(final TreeNode parent, final TreeNode child) {
        return getParent(child) == parent;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isRoot(final TreeNode node) {
        return root == node;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setComparator(final Comparator comparator) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setFilter(final TreeNodeFilter filter) {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setMarked(final TreeNode node, final boolean isMarked) {
        throw new UnsupportedOperationException();
    }

    /**
     * Return the set of marked nodes for this view.
     * @param the unmodifiable set of marked nodes; never null
     */
    public Set getMarked() {
        throw new UnsupportedOperationException();
    }
}
