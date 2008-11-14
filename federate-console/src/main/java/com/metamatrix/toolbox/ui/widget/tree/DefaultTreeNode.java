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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.metamatrix.common.object.ObjectDefinition;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.toolbox.ui.EmptyObject;

/**
This is the default TreeNode used by {@link DefaultTreeModel}.
@since 2.0
@author John P. A. Verhaeg
@version 2.0
*/
public class DefaultTreeNode
    implements Cloneable, TreeNode {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private Object content = null;
    private String name = null;
    private DefaultTreeNode parent = null;
    private ArrayList children = null;
    private ArrayList rootList = null;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a nameless root DefaultTreeNode.
    @since 2.0
    */
    public DefaultTreeNode() {
        this(null, null, null);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeNode(final String name) {
        this(name, null, null);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeNode(final DefaultTreeNode parent) {
        this(null, parent, null);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeNode(final Object content) {
        this(null, null, content);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeNode(final String name, final DefaultTreeNode parent) {
        this(name, parent, null);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeNode(final String name, final Object content) {
        this(name, null, content);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeNode(final Object content, final DefaultTreeNode parent) {
        this(null, parent, content);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeNode(final String name, final DefaultTreeNode parent, final Object content) {
        this.name = name;
        this.parent = parent;
        this.content = content;
        initializeDefaultTreeNode();
    }
    
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized int addChild(final Object child) {
        createChildList();
        final int ndx = children.size();
        if (addChild(child, ndx)) {
            return ndx;
        }
        return -1;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized boolean addChild(Object child, final int index) {
        if (!(child instanceof DefaultTreeNode)) {
            child = new DefaultTreeNode(child.toString());
        }
        createChildList();
        // Wrap count checks around add since ArrayList doesn't provide success feedback for this add method
        final int oldCount = children.size();
        children.add(index, child);
        if (children.size() <= oldCount)    // Less than situation 'should' never occur
            return false;
        ((DefaultTreeNode)child).parent = this;
        return true;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized int addChildren(final Object[] children) {
        createChildList();
        int count = 0;
        for (int ndx = 0;  ndx < children.length;  ndx++) {
            if (addChild(children[ndx]) >= 0) {
                ++count;
            }
        }
        return count;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized int addChildren(final Collection children) {
        createChildList();
        int count = 0;
        final Iterator iterator = children.iterator();
        while (iterator.hasNext()) {
            if (addChild(iterator.next()) >= 0) {
                ++count;
            }
        }
        return count;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @throws CloneNotSupportedException Never thrown
    @since 2.0
    */
    public Object clone()
    throws CloneNotSupportedException {
        return super.clone();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return A negative number, zero, or a positive number depending on whether this node's name is alphabetically less than,
    equal to, or greater then the specified node's name, respectively
    @since 2.0
    */
    public int compareTo(final Object node) {
        return getName().compareTo(((DefaultTreeNode)node).getName());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized boolean contains(final DefaultTreeNode child) {
        if (children == null) {
            return false;
        }
        return children.contains(child);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected synchronized void createChildList() {
        if (children == null) {
            children = new ArrayList();
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return True if this node's name is alphabetically equal to the specified node's name
    @since 2.0
    */
    public boolean equals(final Object node) {
        return this == node;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Not implemented.
    */
    public boolean exists() {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeNode getChild(final int index) {
        return (DefaultTreeNode)children.get(index);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized int getChildCount() {
        if (children == null) {
            return 0;
        }
        return children.size();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized int getChildIndex(final DefaultTreeNode child) {
        return children.indexOf(child);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getChildren() {
        return children;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The object contained within this node.
    @since 2.0
    */
    public Object getContent() {
        return content;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Convenience method for {@link #toString}.  Exists only to satisfy interface requirement.
    @return The name of the node
    @since 2.0
    */
    public String getFullName() {
        return getName();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized Iterator getIterator() {
        if (children == null) {
            return EmptyObject.ITERATOR;
        }
        return children.iterator();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized ListIterator getListIterator() {
        if (children == null) {
            return EmptyObject.LIST_ITERATOR;
        }
        return children.listIterator();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Convenience method for {@link #toString}.
    @since 2.0
    */
    public synchronized String getName() {
        if (name != null) {
            return name;
        }
        if (content != null) {
            return content.toString();
        }
        return toString();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Not implemented.
    @since 2.0
    */
    public String getNamespace() {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTreeNode getParent() {
        return parent;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized DefaultTreeNode getRoot() {
        DefaultTreeNode node = this;
        DefaultTreeNode parent = node.getParent();
        while (parent != null) {
            node = parent;
            parent = node.getParent();
        }
        return node;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized List getRoots() {
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
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Not implemented.
    @since 2.0
    */
    public char getSeparatorChar() {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Not implemented.
    @since 2.0
    */
    public ObjectDefinition getType() {
        throw new UnsupportedOperationException();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected synchronized void initializeDefaultTreeNode() {
        if (parent != null) {
            parent.addChild(this);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized boolean isDescendentOf(final DefaultTreeNode ancestor) {
        DefaultTreeNode descendent = this;
        DefaultTreeNode parent;
        do {
            parent = descendent.getParent();
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
    public synchronized boolean isEmpty() {
        if (children == null) {
            return true;
        }
        return children.isEmpty();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isModified() {
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isRoot() {
        return parent == null;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized int removeAllChildren() {
        if (children == null  ||  children.isEmpty()) {
            return 0;
        }
        int count = 0;
        while (!children.isEmpty()) {
            if (removeChild((DefaultTreeNode)children.get(0)) >= 0) {
                ++count;
            }
        }
        return count;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized int removeChild(final DefaultTreeNode child) {
        if (children == null) {
            return -1;
        }
        int ndx = children.indexOf(child);
        if (ndx < 0  ||  !children.remove(child)) {
            return -1;
        }
        child.parent = null;
        return ndx;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public synchronized DefaultTreeNode removeChild(final int index) {
        final DefaultTreeNode child = (DefaultTreeNode)children.remove(index);
        if (child != null) {
            child.parent = null;
        }
        return child;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the object contained within this node.
    @param content The object contained within this node
    @since 2.0
    */
    public void setContent(final Object content) {
        this.content = content;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the name of this node.
    @param name The name of the node
    @since 2.0
    */
    public void setName(final String name) {
        this.name = name;
    }
}
