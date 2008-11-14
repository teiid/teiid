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

package com.metamatrix.common.tree;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;

public class RuledTreeViewImpl extends AbstractTreeView implements TreeView {

    private List roots = null;
    private TreeView tree = null;
    private ChildRules rules = null;

    public RuledTreeViewImpl(TreeView tree, ChildRules rules ) {
        super();
		ArgCheck.isNotNull(tree);
		ArgCheck.isNotNull(rules);
        this.tree = tree;
        this.rules = rules;
        this.roots = null;
    }

    public RuledTreeViewImpl(TreeView tree, TreeNode root, ChildRules rules ) {
        super();
        ArgCheck.isNotNull(tree);
        ArgCheck.isNotNull(tree);
        ArgCheck.isNotNull(rules);
        // Assert that the root is within the tree ...
        boolean found = false;
        Iterator iter = tree.getRoots().iterator();
        while ( iter.hasNext() ) {
            TreeNode treeRoot = (TreeNode) iter.next();
            if ( tree.isAncestorOf(treeRoot,root) ) {
                found = true;
                break;
            }
        }
        Assertion.assertTrue(found,"The TreeNode must reference a node contained by the TreeView"); //$NON-NLS-1$
        this.tree = tree;
        this.rules = rules;
        this.roots = new ArrayList(1);
        this.roots.add(root);
    }

    protected TreeView getTreeView() {
        return tree;
    }

	// ########################## PropertiedObjectView Methods ###################################

    /**
     * Return the propertied object editor for this view.
     * @return the PropertiedObjectEditor instance
     */
    public PropertiedObjectEditor getPropertiedObjectEditor() {
        return this.tree.getPropertiedObjectEditor();
    }

	// ########################## TreeView Methods ###################################

    /**
     * Get the definitions of the properties for the TreeNode instances
     * returned from this view.
     * @return the unmodifiable list of PropertyDefinition instances; never null
     */
    public List getPropertyDefinitions() {
        throw new RuntimeException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0004));
    }

    /**
     * Returns the single root of this Composite TreeNode system.
     * @return the unmodifiable list of TreeNode instances
     * that represent the root of this view.
     */
    public List getRoots() {
        List theRoots = this.roots;
        if ( theRoots == null ) {
            theRoots = this.tree.getRoots();
        }
        // Find those nodes that are not hidden; if all of the nodes are hidden,
        // then try their children ...
        List results = getUnhiddenNodes(theRoots);
        if ( results.size() != 0 ) {
            return results;
        }
        List modResults = new ArrayList( results );
        Iterator iter = theRoots.iterator();
        while ( iter.hasNext() ) {
            modResults.addAll( this.tree.getChildren((TreeNode)iter.next()) );
        }
        return getUnhiddenNodes(modResults);
    }


    /**
     * Set the roots for this view.
     * @param roots the roots for the view; if null, the view will use the same roots
     * as the view's original tree (model)
     */
    protected void setViewRoots( List roots ) {
        if ( roots == null || roots.isEmpty() ) {
            this.roots = null;
        } else {
            this.roots = roots;
        }
    }

    protected List getUnhiddenNodes( List nodes ) {
        if ( nodes == null ) {
            return Collections.EMPTY_LIST;
        }
        List removeNodes = null;
        Iterator iter = nodes.iterator();
        while ( iter.hasNext() ) {
            TreeNode node = (TreeNode) iter.next();
            if ( isNodeHidden(node) ) {
                if ( removeNodes == null ) {
                    removeNodes = new ArrayList(nodes.size());
                }
                removeNodes.add(node);
            }
        }
        if ( removeNodes == null ) {
            return nodes;
        }
        List results = new ArrayList(nodes.size());
        results.addAll(nodes);
        results.removeAll(removeNodes);
        return Collections.unmodifiableList(results);
    }

    protected Set getUnhiddenNodes( Set nodes ) {
        if ( nodes == null ) {
            return Collections.EMPTY_SET;
        }
        List removeNodes = null;
        Iterator iter = nodes.iterator();
        while ( iter.hasNext() ) {
            TreeNode node = (TreeNode) iter.next();
            if ( isNodeHidden(node) ) {
                if ( removeNodes == null ) {
                    removeNodes = new ArrayList(nodes.size());
                }
                removeNodes.add(node);
            }
        }
        if ( removeNodes == null ) {
            return nodes;
        }
        Set results = new HashSet(7);
        results.addAll(nodes);
        results.removeAll(removeNodes);
        return Collections.unmodifiableSet(results);
    }

    /**
     * Determine whether the specified TreeNode is hidden.
     * @param node the TreeNode instance that is to be checked; may
     * not be null
     * @return true if the node is hidden, or false otherwise.
     */
    public boolean isHidden(TreeNode node) {
        if ( node == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0001));
        }
        return isNodeHidden(node);
    }

    protected boolean isNodeHidden(TreeNode node) {
        if ( this.rules.isHidden(node.getType()) ) {
            return true;
        }
        return ! super.getFilter().accept(node);
    }

    /**
     * Return the marked state of the specified node.
     * @return the marked state of the node.
     */
    public boolean isMarked(TreeNode node) {
    	ArgCheck.isNotNull(node);
        return this.tree.isMarked(node);
    }

    /**
     * Set the marked state of the specified entry.
     * @param true if the node is to be marked, or false if it is to be un-marked.
     */
    public void setMarked(TreeNode node, boolean markedState) {
    	ArgCheck.isNotNull(node);
        this.tree.setMarked(node,markedState);
    }

    /**
     * Return the set of marked nodes for this view.
     * @param the unmodifiable set of marked nodes; never null
     */
    public Set getMarked() {
        return this.getUnhiddenNodes(this.tree.getMarked());
    }

    /**
     * Obtain the TreeNode that represents the home for the underlying system.
     * which for a MetadataTreeView is the same as the root of the view.
     * @return the node that represents the home, or null if no home concept
     * is supported.
     */
    public TreeNode getHome() {
        return this.tree.getHome();
    }

    /**
     * Obtain the abstract path for this TreeNode.
     * @return the string that represents the abstract path of this node; never null
     */
    public String getPath(TreeNode node) {
        return this.tree.getPath(node);
    }

    /**
     * Obtain the character that is used to separate names in a path sequence for
     * the abstract path.  This character is completely dependent upon the implementation.
     * @return the charater used to delimit names in the abstract path.
     */
    public char getSeparatorChar() {
        return this.tree.getSeparatorChar();
    }

    /**
     * Obtain the character (as a String) that is used to separate names in a path sequence for
     * the abstract path.
     * @return the string containing the charater used to delimit names in the abstract path; never null
     */
    public String getSeparator() {
        return this.tree.getSeparator();
    }

    /**
     * Determine the parent TreeNode for the specified node, or null if
     * the specified node is a root.
     * @param node the TreeNode instance for which the parent is to be obtained;
     * may not be null
     * @return the parent node, or null if there is no parent
     */
    public TreeNode getParent(TreeNode node) {
        // If the node is one of the roots, then return null
        if ( this.roots != null && this.roots.contains(node) ) {
            return null;
        }
        // Otherwise, find the parent from the tree
        return this.tree.getParent(node);
    }

    /**
     * Determine whether the specified TreeNode may contain children.
     * @param entry the TreeNode instance that is to be checked; may
     * not be null
     * @return true if the entry can contain children, or false otherwise.
     */
    public boolean allowsChildren(TreeNode entry) {
    	ArgCheck.isNotNull(entry);
        return this.rules.getAllowsChildren(entry.getType());
    }

    /**
     * Determine whether the specified parent TreeNode may contain the
     * specified child node.
     * @param parent the TreeNode instance that is to be the parent;
     * may not be null
     * @param potentialChild the TreeNode instance that is to be the child;
     * may not be null
     * @return true if potentialChild can be placed as a child of parent,
     * or false otherwise.
     */
    public boolean allowsChild(TreeNode parent, TreeNode potentialChild) {
    	ArgCheck.isNotNull(parent);
    	ArgCheck.isNotNull(potentialChild);
        return this.rules.getAllowsChild(parent.getType(),potentialChild.getType());
    }

    /**
     * Obtain the set of entries that are considered the children of the specified
     * TreeNode.
     * @param parent the TreeNode instance for which the child entries
     * are to be obtained; may not be null
     * @return the unmodifiable list of TreeNode instances that are considered the children
     * of the specified node; never null but possibly empty
     */
    public List getChildren(TreeNode parent) {
    	ArgCheck.isNotNull(parent);
        return this.getUnhiddenNodes( this.tree.getChildren(parent) );
    }

//    public int getIndexOfChild( TreeNode child ) {
//        Assertion.isNotNull(child,"The TreeNode reference may not be null");
//        return this.tree.getIndexOfChild(child);
//    }

    /**
     * Determine whether the specified node is a child of the given parent node.
     * @return true if the node is a child of the given parent node.
     */
    public boolean isParentOf(TreeNode parent, TreeNode child) {
    	ArgCheck.isNotNull(parent);
    	ArgCheck.isNotNull(child);
        // If the child is one of the roots, then return false
        if ( this.roots != null && this.roots.contains(child) ) {
            return false;
        }
        return this.tree.isParentOf(parent,child);
    }

    /**
     * Determine whether the specified node is a descendent of the given ancestor node.
     * @return true if the node is a descendent of the given ancestor node.
     */
    public boolean isAncestorOf(TreeNode ancestor, TreeNode descendent) {
    	ArgCheck.isNotNull(ancestor);
    	ArgCheck.isNotNull(descendent);
        // If the descendent is one of the roots, then return false
        if ( this.roots != null && this.roots.contains(descendent) ) {
            return false;
        }
        return this.tree.isAncestorOf(ancestor,descendent);
    }

    /**
     * Return the tree node editor for this view.
     * @return the TreeNodeEditor instance
     */
    public TreeNodeEditor getTreeNodeEditor() {
        return this.tree.getTreeNodeEditor();
    }

	// ########################## UserTransactionFactory Methods ###################################

    /**
     * Create a new instance of a UserTransaction that may be used to
     * read information.  Read transactions do not have a source object
     * associated with them (since they never directly modify data).
     * @return the new transaction object
     */
    public UserTransaction createReadTransaction() {
        return this.tree.createReadTransaction();
    }

    /**
     * Create a new instance of a UserTransaction that may be used to
     * write and/or update information.  The transaction will <i>not</i> have a source object
     * associated with it.
     * @return the new transaction object
     */
    public UserTransaction createWriteTransaction() {
        return this.tree.createWriteTransaction();
    }

    /**
     * Create a new instance of a UserTransaction that may be used to
     * write and/or update information. The source object will be used for all events that are
     * fired as a result of or as a product of this transaction.
     * @param source the object that is considered to be the source of the transaction;
     * may be null
     * @return the new transaction object
     */
    public UserTransaction createWriteTransaction(Object source) {
        return this.tree.createWriteTransaction(source);
    }

    /**
     * Obtain an iterator for this whole view, which navigates the view's
     * nodes using pre-order rules (i.e., it visits a node before its children).
     * @return the view iterator
     */
    public Iterator iterator() {
        return new TreeNodeIterator(this.getRoots(),this);
    }

    /**
     * Obtain an iterator for the view starting at the specified node.  This
     * implementation currently navigates the subtree using pre-order rules
     * (i.e., it visits a node before its children).
     * @param startingPoint the root of the subtree over which the iterator
     * is to navigate; may not be null
     * @return the iterator that traverses the nodes in the subtree starting
     * at the specified node; never null
     */
    public Iterator iterator(TreeNode startingPoint) {
    	ArgCheck.isNotNull(startingPoint);
        return new TreeNodeIterator(startingPoint,this);
    }

    protected ChildRules getChildRules() {
        return this.rules;
    }

    public void print( PrintStream stream ) {
        if ( stream == null ) {
            throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0010));
        }
        stream.println("ProxyTreeViewImpl"); //$NON-NLS-1$
        Iterator iter = this.getRoots().iterator();
        while ( iter.hasNext() ) {
            print((TreeNode)iter.next(),stream,"  "); //$NON-NLS-1$
        }
    }

    private void print( TreeNode node, PrintStream stream, String leadingString ) {
        if ( this.isNodeHidden(node) ) {
            return;
        }
        stream.println(leadingString + node.getName() );
        Iterator iter = this.getChildren(node).iterator();
        while ( iter.hasNext() ) {
            print((TreeNode)iter.next(),stream,leadingString + "  "); //$NON-NLS-1$
        }
    }

}
