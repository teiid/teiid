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

package com.metamatrix.common.tree;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;


/**
 * This iterator implementation extends {@link com.metamatrix.common.tree.TreeNodeIterator} by maintaining
 * a stack of TreeNodes from the root to the last node returned by {@link #next()}.    
 * <p>
 * This class has the ability to track with each TreeNode instance in the stack a payload object.  However,
 * in order to use this feature, this class must be extended by subclassing the {@link #createPayload(TreeNode)}
 * method must be overridden.
 * </p>
 * <p>
 * Also, the {@link java.util.LinkedList stack} is accessed with the {@link #getStack()} method and contains
 * instances of {@link TreeNodeIteratorWithStack#StackEntry TreeNodeIteratorWithStack.StackEntry} instances.
 * </p>
 */
public class TreeNodeIteratorWithStack implements Iterator {

    private final Iterator iterator;
    private final LinkedList workingList;
    private final LinkedList stack;
    private final TreeView view;
    private final TreeNode startingNode;

    /**
     * Construct an instance of XMLNamespaceAwareIterator.
     * @param startingNode
     * @param view
     */
    public TreeNodeIteratorWithStack(final TreeNode startingNode, final TreeView view) {
        this(startingNode,view, new TreeNodeIterator(startingNode,view));
    }

    /**
     * Construct an instance that wraps the supplied iterator.
     * @param startingNode
     * @param view
     * @param iter the iterator that provides the set of TreeNodes over which the iteration is to occur.
     */
    public TreeNodeIteratorWithStack(final TreeNode startingNode, final TreeView view, final Iterator iter) {
        this.iterator = iter;
        this.view = view;
        this.workingList = new LinkedList();
        this.stack = new LinkedList();
        this.startingNode = startingNode;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#remove()
     */
    public void remove() {
        this.iterator.remove();
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    public Object next() {
        final Object nextObj = this.iterator.next();
        process(nextObj);
        return nextObj;
    }

    protected void process( final Object object ) {
        synchronizeStack((TreeNode)object);
    }

    /**
     * @param entity
     */
    protected void synchronizeStack( final TreeNode obj ) {
        // populate the working list with the ancestor path ...
        this.workingList.clear();
        TreeNode node = obj;
        while ( node != null ) {
            this.workingList.add(0,node);
            if ( isStartingNode(node) ) {
                node = null;
            } else {
                node = view.getParent(node);
            }
        }
        
        // Go down from the stack until the paths diverge
        TreeNode objAncestor = null;
        final ListIterator workingIter = this.workingList.listIterator();
        final ListIterator stackIter = this.stack.listIterator();
        while (workingIter.hasNext() && stackIter.hasNext() ) {
            objAncestor = (TreeNode)workingIter.next();
            final StackEntry entry = (StackEntry) stackIter.next();
            if ( entry.getTreeNode() != objAncestor ) {
                // Clear the rest of the stack ...
                while ( this.stack.removeLast() != entry ) {
                }
                // back up the workingIter since we didn't do anything with objAncestor yet
                workingIter.previous();
                break;
            }
        }
        
        // Add whatever is left in the workingIter
        while ( workingIter.hasNext() ) {
            objAncestor = (TreeNode)workingIter.next();
            final Object payload = createPayload(objAncestor);
            final StackEntry entry = new StackEntry(objAncestor, payload);
            this.stack.add(entry);
        }
    }
    
    /**
     * @param node
     * @return
     */
    protected boolean isStartingNode(TreeNode node) {
        if ( this.startingNode != null ) {
            return this.startingNode == node;
        }
        if ( this.iterator instanceof TreeNodeIterator ) {
            return ((TreeNodeIterator)this.iterator).isStartingNode(node);
        }
        return false;
    }

    protected Object createPayload( final TreeNode node ) {
        return null;
    }
    
    /**
     * Return the list of {@link TreeNodeIteratorWithStack.StackEntry StackEntry} objects that each have
     * a target and a payload.
     * @return
     */
    public LinkedList getStack() {
        return this.stack;
    }
    
    /**
     * @return
     */
    public TreeView getTreeView() {
        return view;
    }
    
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        int counter = 0;
        final Iterator iter = getStack().iterator();
        while (iter.hasNext()) {
            final StackEntry entry = (StackEntry)iter.next();
            sb.append("  "); //$NON-NLS-1$
            sb.append(++counter);
            sb.append(". "); //$NON-NLS-1$
            sb.append(entry.getTreeNode().getName());
            final Object payload = entry.getPayload();
            if ( payload != null ) {
                sb.append( toStringPayload(payload) );
            }
            sb.append("\n"); //$NON-NLS-1$
        }
        return sb.toString();
    }
    
    protected String toStringPayload( final Object obj ) {
        return ""; //$NON-NLS-1$
    }

    public static class StackEntry {
        private final TreeNode target;
        private final Object payload;
        protected StackEntry( final TreeNode target, final Object payload ) {
            this.target = target;
            this.payload = payload;
        }
        /**
         * Return the tree node.
         * @return the node; never null
         */
        public TreeNode getTreeNode() {
            return this.target;
        }
        /**
         * Return the payload for the node in the stack.
         * @return the payload; may be null
         */
        public Object getPayload() {
            return payload;
        }
        public String toString() {
            return this.target.toString();
        }

    }

}
