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

package com.metamatrix.console.ui.util;

import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.MutableTreeNode;

import com.metamatrix.console.ui.tree.ChildSortingTreeNode;


/**
 * LazyBranchNode is a specialization of DefaultMutableTreeNode that delays
 * populating its child nodes until it is expanded.
 *
 * To use, extend this class (typically as an inner class) and implement the
 * populate method to obtain the child nodes, construct new LazyBranchNodes
 * for each child branch and new DefaultMutableTreeNodes for each leaf, and call
 * add(newChild) for each child node.
 *
 * Example:
 * <pre>
 *   class MyLazyNode extends LazyBranchNode {
 *       MyLazyNode(Object id) {
 *           super(id);
 *       }
 *       public void populate() {
 *           if ( ! isPopulated() ) {  // will be true if add() has ever been called on this node
 *               Iterator iter = MyDataManager.getChildrenOfNode(id).iterator();
 *               while ( iter.hasNext() ) {
 *                   Object obj = iter.next();
 *                   if ( obj instanceof ALeafNodeClass ) {
 *                       this.add( new DefaultMutableTreeNode(node) );  // leaf nodes
 *                   } else {
 *                       this.add( new ResourceNode(node) );            // branch nodes
 *                   }
 *               }
 *           }
 *       }
 *   }
 * </pre>
 *
 * @see LazyBranchListener
 */
public abstract class LazyBranchNode extends ChildSortingTreeNode {

    protected Object id;
    protected boolean populated = false;

    public LazyBranchNode(DefaultTreeModel treeModel, Object id) {
        super(treeModel);
        setUserObject(id);
        this.id = id;
        //super.add(new DefaultMutableTreeNode("(empty)"));
    }

    public void add(MutableTreeNode newChild) {
        if ( !populated ) {
            removeAllChildren();
            populated = true;
        }
        super.add(newChild);
    }

    public boolean isLeaf() { return false; }

    public boolean isPopulated() { return populated; }

    public void setPopulated() { populated = true; }
    
    public void resetPopulated() { populated = false; }

    abstract public void populate() throws Exception;
}


