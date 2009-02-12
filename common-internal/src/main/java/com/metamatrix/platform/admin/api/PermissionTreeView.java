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

package com.metamatrix.platform.admin.api;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public interface PermissionTreeView {
    /**
     * Reset all tree nodes to their original <code>AuthorizationActions</code> values.
     */
    void resetTree();
    /**
     * Determines whether methods in this class return <i>hidden</i> nodes in results. The state can
     * be toggled on or off as needed. The default is <code>false</code> (don't show hidden nodes).
     * @param showHidden If <code>true</code>, hidden nodes will be returned in results, if
     * <code>false</code>, they will not.
     */
    void setShowHidden( boolean showHidden );

    /**
     * (Un)Mark this node.
     * @param entry The node to mark or unmark.
     * @param markedState If <code>true</code>, the node will be marked, if <code>false</code>,
     * the node will be unmarked.
     */
    void setMarked( PermissionNode entry, boolean markedState );

    /**
     * Set the subtree rooted at this node as hidden.
     * @see setShowHidden
     * @param entry The node to mark or unmark.
     */
    void setBranchHidden( PermissionNode startingPoint );

    /**
     * Obtain a depth-first <code>Iterator</code> starting at the given node.
     * @return The iterator.
     */
    Iterator iterator( PermissionNode startingPoint );

    /**
     * Obtain a depth-first <code>Iterator</code> over the whole tree starting at the root.
     * @return The iterator.
     */
    Iterator iterator();

    /**
     * Obtain a breadth-first <code>Iterator</code> starting at the given node.
     * @return The iterator.
     */
    Iterator breadthFirstIterator( PermissionNode startingPoint );

    /**
     * Obtain a breadth-first <code>Iterator</code> over the whole tree starting at the root.
     * @return The iterator.
     */
    Iterator breadthFirstIterator();

    /**
     * Obtain the root <code>PermissionDataNode</code> of the tree.  The root of this tree is always
     * hidden but will <i>always</i> be returned even if the state of <code>showHidden</code> is
     * <code>false</code>.
     * @return The root of the tree.
     */
    PermissionNode getRoot();

    /**
     * Obtain a list of <code>PermissionDataNode</code>s which are the children of the root.  These
     * nodes are the <i>real</i> roots of the tree, since the root is just a placeholder that holds
     * the real roots.
     * @return The list of <code>PermissionDataNode</code>s that are the roots of the forest.
     */
    List getRoots();

    /**
     * Determine the parent <code>PermissionDataNode</code> for the specified entry, or null if
     * the specified entry is a root.
     * @param entry the <code>PermissionDataNode</code> instance for which the parent is to be obtained;
     * may not be null
     * @return the parent entry, or null if there is no parent
     */
    PermissionNode getParent(PermissionNode entry);

    /**
     * Obtain the set of entries that are considered the children of the specified
     * <code>PermissionDataNode</code>.
     * @param parent the <code>PermissionDataNode</code> instance for which the child entries
     * are to be obtained; may not be null
     * @return the unmodifiable list of <code>PermissionDataNode</code> instances that are considered
     * the children of the specified entry; never null but possibly empty
     */
    List getChildren(PermissionNode parent);

    /**
     * Determine whether the given <code>descendant</code> is a descendant of the given
     * <code>ancestor</code>.<br></br>
     * This method will check <i>all</i> descendants of the ancester, even if they are marked hidden.
     * @param ancestor The node to check to see if it is an ancestor of the <code>descendant</code>.
     * @param descendant The node to check to see if it is a descendant <code>ancestor</code>.
     * @return <code>true</code> if <code>ancestor</code> is the ancestor of <code>descendant</code>.
     */
    boolean isDescendantOf( PermissionNode ancestor, PermissionNode descendant );

    /**
     * Obtain all the <i>marked</i> <code>PermissionDataNode</code>s in the tree. Note that the
     * nodes are not nessesarily in tree form. They are just a collection of nodes.<br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @return The <code>Set</code> of all marked nodes in the tree.
     */
    Set getMarked();

    /**
     * Obtain all the <i>marked</i> <code>PermissionDataNode</code>s in the tree <i>under</i> the
     * given node. Note that the nodes are not nessesarily in tree form. They are just a
     * collection of nodes.
     * <br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @return The <code>Set</code> of all marked nodes in the tree below <code>startingPoint</code>.
     */
    Set getMarkedDescendants( PermissionNode startingPoint );

    /**
     * Obtain all the <code>PermissionDataNode</code>s in the tree <i>under</i> the given
     * node.
     * <br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @param startingPoint The point in the tree to start the search for descendants.
     * @return The <code>List</code> of all nodes in the tree below <code>startingPoint</code>.
     */
    List getDescendants( PermissionNode startingPoint );

    /**
     * Obtain all the <i>modified</i> <code>PermissionDataNode</code>s in the tree.<br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @return The <code>List</code> of all modified nodes in the tree.
     */
    List getModified();

    /**
     * Obtain the <i>modified</i> <code>PermissionDataNode</code>s closest to the root in the tree.
     * <br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @return The <code>List</code> of all top level modified nodes in the tree.
     */
    List getModifiedBreadthFirst();

    /**
     * Obtain all the <i>modified</i> <code>PermissionDataNode</code>s in the tree <i>under</i> the
     * given node. Note that the nodes are not nessesarily in tree form.
     * <br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @return The <code>List</code> of all modified nodes in the tree below <code>startingPoint</code>.
     */
    List getModifiedDescendants( PermissionNode startingPoint );

    /**
     * Obtain all the <i>unmodified</i> <code>PermissionDataNode</code>s in the tree <i>under</i> the
     * given node. Note that the nodes are not nessesarily in tree form.
     * <br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @return The <code>List</code> of all unmodified nodes in the tree below <code>startingPoint</code>.
     */
    List getUnModifiedDescendants( PermissionNode startingPoint );
}
