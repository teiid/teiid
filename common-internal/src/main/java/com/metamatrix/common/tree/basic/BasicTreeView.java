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

package com.metamatrix.common.tree.basic;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.tree.PassThroughTreeNodeFilter;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;
import com.metamatrix.common.tree.TreeNodeFilter;
import com.metamatrix.common.tree.TreeNodeIterator;
import com.metamatrix.common.tree.TreeNodePathComparator;
import com.metamatrix.common.tree.TreeNodeSource;
import com.metamatrix.common.tree.TreeView;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.Assertion;

public class BasicTreeView implements TreeView {
    private static TreeNodeFilter DEFAULT_FILTER = new PassThroughTreeNodeFilter();
    private static Comparator DEFAULT_COMPARATOR = new TreeNodePathComparator();
    private TreeNodeFilter filter = DEFAULT_FILTER;
    private Comparator comparator = DEFAULT_COMPARATOR;

    private BasicTreeNode root;
    private BasicTreeNodeSource source;
    private BasicTreeNodeEditor editor;
    private List unmodifiableRoot;
    private String delimiter;

    public BasicTreeView(TreeNode root, TreeNodeSource source) {
        Assertion.isNotNull(source,"The BasicTreeNodeSource reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue(source instanceof BasicTreeNodeSource,"The TreeNodeSource must be a BasicTreeNodeSource"); //$NON-NLS-1$
        this.root = this.assertBasicTreeNode(root);
        this.source = (BasicTreeNodeSource)source;

        List viewRoot = new ArrayList(1);
        viewRoot.add(this.root);
        this.unmodifiableRoot = Collections.unmodifiableList(viewRoot);
    }

    protected BasicTreeNode assertBasicTreeNode( TreeNode node ) {
        Assertion.isNotNull(node, "The TreeNode reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue(node instanceof BasicTreeNode, "The referenced object is not an BasicTreeNode"); //$NON-NLS-1$
        BasicTreeNode basicNode = (BasicTreeNode) node;
        return basicNode;
    }

    /**
     * Set the filter that limits the set of TreeNode instances returned from this view.
     * @param filter the filter, or null if the default "pass-through" filter should be used.
     */
    public void setFilter(TreeNodeFilter filter) {
        if ( filter == null ) {
            this.filter = DEFAULT_FILTER;
        } else {
            this.filter = filter;
        }
    }

    /**
     * Return the filter that is being used by this view.
     * @return the current filter; never null
     */
    public TreeNodeFilter getFilter() {
        return this.filter;
    }

    /**
     * Set the comparator that should be used to order the children.
     * @param comparator the comparator, or null if node path sorting should be used.
     */
    public void setComparator(Comparator comparator) {
        if ( comparator == null ) {
            this.comparator = DEFAULT_COMPARATOR;
        } else {
            this.comparator = comparator;
        }
    }

    /**
     * Return the comparator used to order for children returned from this view.
     * @return the current comparator; never null
     */
    public Comparator getComparator() {
        return this.comparator;
    }

    /**
     * Obtain an iterator for this whole view, which navigates the view's
     * nodes using pre-order rules (i.e., it visits a node before its children).
     * @return the view iterator
     */
    public Iterator iterator() {
        return new TreeNodeIterator(this.root,this);
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
        Assertion.isNotNull(startingPoint,"The TreeNode reference may not be null"); //$NON-NLS-1$
        return new TreeNodeIterator(startingPoint,this);
    }

    /**
     * Get the definitions of the properties for the TreeNode instances
     * returned from this view.
     * @return the unmodifiable list of PropertyDefinition instances; never null
     */
    public List getPropertyDefinitions() {
        return this.getTreeNodeEditor().getPropertyDefinitions(this.root);
    }

    /**
     * Returns the single root of this TreeNode system.
     * @return the unmodifiable list of TreeNode instances
     * that represent the root of this view.
     */
    public List getRoots() {
        return this.unmodifiableRoot;
    }

    /**
     * Determine whether the specified TreeNode is a root of the underlying system.
     * @param node the TreeNode instance that is to be checked; may not be null
     * @return true if the node is a root, or false otherwise.
     */
    public boolean isRoot(TreeNode node) {
        BasicTreeNode basicNode = this.assertBasicTreeNode(node);
        return this.getRoots().contains(basicNode);
    }

    /**
     * Determine whether the specified TreeNode may contain children.
     * @param entry the TreeNode instance that is to be checked; may
     * not be null
     * @return true if the entry can contain children, or false otherwise.
     */
    public boolean allowsChildren(TreeNode node) {
        return this.source.allowsChildren(node);
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
        return this.source.allowsChild(parent,potentialChild);
    }

    /**
     * Determine whether the specified TreeNode is hidden.
     * @param node the TreeNode instance that is to be checked; may not be null
     * @return true if the node is hidden, or false otherwise.
     */
    public boolean isHidden(TreeNode node) {
        return false;
    }

    /**
     * Return the marked state of the specified node.
     * @return the marked state of the node.
     */
    public boolean isMarked(TreeNode node) {
        BasicTreeNode basicNode = this.assertBasicTreeNode(node);
        return basicNode.isMarked();
    }

    /**
     * Set the marked state of the specified entry.
     * @param true if the node is to be marked, or false if it is to be un-marked.
     */
    public void setMarked(TreeNode node, boolean markedState) {
        BasicTreeNode basicNode = this.assertBasicTreeNode(node);
        basicNode.setMarked(markedState);
    }

    /**
     * Return the set of marked nodes for this view.
     * @param the unmodifiable set of marked nodes; never null
     */
    public Set getMarked() {
        Set result = new HashSet();
        addMarkedNodesToSet(this.root, result);
        return Collections.unmodifiableSet( result );
    }

    /**
     * Return the set of marked nodes for this view.
     * @param the unmodifiable set of marked nodes; never null
     */
    private void addMarkedNodesToSet( BasicTreeNode parent, Set result ) {
        if ( parent.isMarked() ) {
            result.add(parent);
        }

        BasicTreeNode child = null;
        Iterator iter = parent.getChildren().iterator();
        while ( iter.hasNext() ) {
            child = (BasicTreeNode) iter.next();
            addMarkedNodesToSet( child, result );
        }
    }

    /**
     * Obtain the TreeNode that represents the home for the view.
     * @return the node that represents the home, or null if no home concept
     * is supported.
     */
    public TreeNode getHome() {
        return this.root;
    }

    /**
     * Obtain the abstract path for this TreeNode.
     * @return the string that represents the abstract path of this node; never null
     */
    public String getPath(TreeNode node) {
        BasicTreeNode basicNode = this.assertBasicTreeNode(node);
        if ( this.delimiter != null ) {
            return basicNode.getFullName(this.delimiter);
        }
        return basicNode.getFullName();
    }

    /**
     * Obtain the character that is used to separate names in a path sequence for
     * the abstract path.  This character is completely dependent upon the implementation.
     * @return the charater used to delimit names in the abstract path.
     */
    public char getSeparatorChar() {
        if ( this.delimiter != null ) {
            return this.delimiter.charAt(0);
        }
        return this.root.getSeparatorChar();
    }

    /**
     * Obtain the character (as a String) that is used to separate names in a path sequence for
     * the abstract path.
     * @return the string containing the charater used to delimit names in the abstract path; never null
     */
    public String getSeparator() {
        if ( this.delimiter != null ) {
            return this.delimiter;
        }
        return this.root.getSeparator();
    }

    public void setSeparator( String delimiter ) {
        if ( delimiter == null ) {
            this.delimiter = null;
            return;
        }
        Assertion.assertTrue(delimiter.length()==1,"The delimiter string must be a single character"); //$NON-NLS-1$
        this.delimiter = delimiter;
    }

    /**
     * Determine the parent TreeNode for the specified node, or null if
     * the specified node is a root.
     * @param node the TreeNode instance for which the parent is to be obtained;
     * may not be null
     * @return the parent node, or null if there is no parent
     */
    public TreeNode getParent(TreeNode node) {
        return this.source.getParent(node);
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
        return this.source.getChildren(parent);
    }

    /**
     * Determine whether the specified node is a child of the given parent node.
     * @return true if the node is a child of the given parent node.
     */
    public boolean isParentOf(TreeNode parent, TreeNode child) {
        return this.source.isParentOf(parent,child);
    }

    public int getIndexOfChild( TreeNode child ) {
        Assertion.isNotNull(child,"The TreeNode reference may not be null"); //$NON-NLS-1$
        BasicTreeNode childNode  = this.assertBasicTreeNode(child);
        BasicTreeNode parentNode = childNode.getParent();
        if ( parentNode != null ) {
            return parentNode.getIndexOfChild(childNode);
        }

        if ( child == this.getHome() ) {
            return -1;
        }
        throw new AssertionError(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0029, childNode.getFullName(this.getSeparator()) ));
    }

    /**
     * Determine whether the specified node is a descendent of the given ancestor node.
     * @return true if the node is a descendent of the given ancestor node.
     */
    public boolean isAncestorOf(TreeNode ancestor, TreeNode descendent) {
        return this.source.isAncestorOf(ancestor, descendent);
    }

    /**
     * Return the propertied object editor for this view.
     * @return the PropertiedObjectEditor instance
     */
    public PropertiedObjectEditor getPropertiedObjectEditor() {
        return this.getTreeNodeEditor();
    }

    /**
     * Return the tree node editor for this view.
     * @return the TreeNodeEditor instance
     */
    public TreeNodeEditor getTreeNodeEditor() {
        if (this.editor == null) {
            this.editor = (BasicTreeNodeEditor)this.source.createTreeNodeEditor();
        }
        return editor;
    }

	// ########################## Implementation Methods ###################################

    /**
     * Lookup the BasicTreeNode Object(s) referenced by the relative path in this view.
     * Since this implementation allows some nodes in the same parent to have
     * the same name (restrictions apply), so this method can return multiple
     * BasicTreeNode instances.
     * <p>This method ignores case.
     * @param path the path of the desired node specified in terms of this view
     * (i.e., the result of calling <code>getPath()</code> on this view with the
     * returned node as the parameter should result in the same value as <code>path</code>);
     * may not be null or zero-length
     * @return the collection of BasicTreeNode instances that have the specified path;
     * may be empty if no BasicTreeNode exists with the path.
     * @throws AssertionError if the path is null or zero-length
     */
    public Collection lookup( String path ) {
        Assertion.isNotNull(path,"The path reference may not be null"); //$NON-NLS-1$
        Assertion.isNotZeroLength(path,"The path may not be zero-length"); //$NON-NLS-1$

        // If the path specifies the root node ...
        if ( this.root.getName().equals(path) ) {
            List result = new ArrayList(1);
            result.add(this.root);
            return result;
        }

        // Otherwise, strip off the root name and continue ...
        String childPath = path;
        String rootPath = root.getName() + this.getSeparator();
        if ( path.startsWith(rootPath) ) {
            childPath = path.substring(rootPath.length());
        }

        return this.root.getDecendant(childPath,this.getSeparator(),true);
    }

    /**
     * Returns the single root of this TreeView.
     * @return the TreeNode instance that represent the root of this view.
     */
    protected TreeNode getRoot() {
        return this.root;
    }

    /**
     * Set the single root used for this TreeView.
     * @param root the TreeNode instance to be used as the root of this view.
     * @return whether the root was successfully set
     */
    protected boolean setRoot( TreeNode root ) {
        Assertion.isNotNull(root,"The BasicTreeNodeSource reference may not be null"); //$NON-NLS-1$
        Assertion.assertTrue(root instanceof BasicTreeNodeSource,"The TreeNodeSource must be a BasicTreeNodeSource"); //$NON-NLS-1$
        this.root = this.assertBasicTreeNode(root);
        return true;
    }

    /**
     * Returns the TreeNodeSource associated with this TreeView.
     * @return the TreeNodeSource instance of this view.
     */
    protected TreeNodeSource getTreeNodeSource() {
        return this.source;
    }

    public void print( PrintStream stream ) {
        print(stream,false);
    }

    public void print( PrintStream stream, boolean showMarked ) {
        Assertion.isNotNull(stream,"The stream reference may not be null"); //$NON-NLS-1$
        stream.println("BasicTreeView"); //$NON-NLS-1$
        print(this.root,stream,"  ",showMarked); //$NON-NLS-1$
    }

    private void print( BasicTreeNode node, PrintStream stream, String leadingString, boolean showMarked ) {
        String markedString = ""; //$NON-NLS-1$
        if ( showMarked ) {
            markedString = ( node.isMarked() ? " <marked>" : " <unmarked>" ); //$NON-NLS-1$ //$NON-NLS-2$
        }
        stream.println(leadingString + node.getName() + markedString );
        Iterator iter = this.getChildren(node).iterator();
        while ( iter.hasNext() ) {
            print((BasicTreeNode)iter.next(),stream,leadingString + "  ", showMarked); //$NON-NLS-1$
        }
    }

    public static String collectionToString( Collection objs ) {
        StringBuffer sb = new StringBuffer(100);
        sb.append("{"); //$NON-NLS-1$
        int counter = 0;
        Iterator itr = objs.iterator();
        while (itr.hasNext()) {
            if (counter > 0) {
                sb.append(","); //$NON-NLS-1$
            }
            sb.append(itr.next().toString());
            counter++;
        }
        sb.append("}"); //$NON-NLS-1$
        return sb.toString();
    }

}

