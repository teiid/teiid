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

package com.metamatrix.platform.admin.apiimpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.metamatrix.admin.AdminMessages;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.platform.admin.api.PermissionDataNodeTreeView;
import com.metamatrix.platform.admin.api.PermissionNode;
import com.metamatrix.platform.admin.api.exception.PermissionNodeNotFoundException;
import com.metamatrix.platform.security.api.AuthorizationActions;
import com.metamatrix.platform.security.api.AuthorizationPermission;
import com.metamatrix.platform.security.api.BasicAuthorizationPermission;
import com.metamatrix.platform.security.api.SecurityPlugin;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;

/**
 * Provides a <i>view</i> into the tree of <code>PermissionDataNode</code>s and supports utility
 * methods that perform funcions on the tree as a whole.
 * <p>
 * A property can be set (or toggled) to determine if clients want methods to return nodes marked
 * hidden. See {@link #setShowHidden}.
 * </p>
 */
public class PermissionDataNodeTreeViewImpl implements PermissionDataNodeTreeView {

    // Map of resourceName -> PermissionDataNodeImpl to speed lookup durring init
    Map resourceLookUp;

    // The root of the forest is fake. Just used to commonly root the forest. It's allways hidden.
    private PermissionDataNodeImpl fakeRoot;

    // Determines whether methods return hidden nodes in results.  Default is false.
    private boolean showHidden;

    /**
    * Construct a <code>PermissionDataNode</code>.
    */
    public PermissionDataNodeTreeViewImpl(PermissionNode root) {
        fakeRoot = assertPermissionDataNode(root);
        // Hide the root
        fakeRoot.setHidden(true);
        fakeRoot.setAllowedActions(StandardAuthorizationActions.NONE);
        showHidden = false;
    }

    /**
     * Perform safe runtime casts.
     * @param node The node to check.
     * @return The safley cast node.
     */
    protected PermissionDataNodeImpl assertPermissionDataNode( PermissionNode node ) {
        if(node == null){
            Assertion.isNotNull(node, SecurityPlugin.Util.getString(AdminMessages.ADMIN_0018, "PermissionDataNode")); //$NON-NLS-1$
        }
        if(!(node instanceof PermissionDataNodeImpl) ){
            Assertion.assertTrue(node instanceof PermissionDataNodeImpl, SecurityPlugin.Util.getString(AdminMessages.ADMIN_0041, "PermissionDataNode")); //$NON-NLS-1$
        }
        PermissionDataNodeImpl dataNode = (PermissionDataNodeImpl) node;
        return dataNode;
    }

    /**
     * Determines whether methods in this class return <i>hidden</i> nodes in results. The state can
     * be toggled on or off as needed. The default is <code>false</code> (don't show hidden nodes).
     * @param showHidden If <code>true</code>, hidden nodes will be returned in results, if
     * <code>false</code>, they will not.
     */
    public void setShowHidden( boolean showHidden ) {
        this.showHidden = showHidden;
    }

    /**
     * Hide all nodes whose resource name starts with "System".
     */
    public void hideAllSystemNodes() {
        List children = fakeRoot.getChildren();
        for ( int i=0; i<children.size(); i++ ) {
            PermissionDataNodeImpl aNode = (PermissionDataNodeImpl) children.get(i);
            if ( aNode.getResourceName().startsWith("System") ) { //$NON-NLS-1$
                this.setBranchHidden(aNode);
            }
        }
    }

    /**
     * (Un)Mark this node.
     * @param entry The node to mark or unmark.
     * @param markedState If <code>true</code>, the node will be marked, if <code>false</code>,
     * the node will be unmarked.
     */
    public void setMarked( PermissionNode entry, boolean markedState ) {
        PermissionDataNodeImpl theEntry = this.assertPermissionDataNode(entry);
        theEntry.setMarked(markedState, false);
    }

    /**
     * Obtain a depth-first <code>Iterator</code> starting at the given node.
     * @return The iterator.
     */
    public Iterator iterator( PermissionNode startingPoint ) {
        PermissionDataNodeImpl theRoot = this.assertPermissionDataNode(startingPoint);
        List nodes = new ArrayList();
        // true => include root
        fillNodeList(theRoot, showHidden, true, nodes);
        return nodes.iterator();
    }

    /**
     * Obtain a depth-first <code>Iterator</code> over the whole tree starting at the root.
     * @return The iterator.
     */
    public Iterator iterator() {
        List nodes = new ArrayList();
        // true => include root
        fillNodeList(fakeRoot, showHidden, true, nodes);
        return nodes.iterator();
    }

    /**
     * Obtain a breadth-first <code>Iterator</code> starting at the given node.
     * @return The iterator.
     */
    public Iterator breadthFirstIterator( PermissionNode startingPoint ) {
        PermissionDataNodeImpl theRoot = this.assertPermissionDataNode(startingPoint);
        // true => include root
        List nodes = fillNodeListBreadthFirst(theRoot, showHidden, true);
        return nodes.iterator();
    }

    /**
     * Obtain a breadth-first <code>Iterator</code> over the whole tree starting at the root.
     * @return The iterator.
     */
    public Iterator breadthFirstIterator() {
        // true => include root
        List nodes = fillNodeListBreadthFirst(fakeRoot, showHidden, true);
        return nodes.iterator();
    }

    /**
     * Obtain the root <code>PermissionDataNode</code> of the tree.  The root of this tree is always
     * hidden but will <i>always</i> be returned even if the state of <code>showHidden</code> is
     * <code>false</code>.
     * @return The root of the tree.
     */
    public PermissionNode getRoot() {
        return fakeRoot;
    }

    /**
     * Obtain a list of <code>PermissionDataNode</code>s which are the children of the root.  These
     * nodes are the <i>real</i> roots of the tree, since the root is just a placeholder that holds
     * the real roots.
     * @return The list of <code>PermissionDataNode</code>s that are the roots of the forest.
     */
    public List getRoots() {
        return fakeRoot.getChildren();
    }

    /**
     * Determine the parent <code>PermissionDataNode</code> for the specified entry, or null if
     * the specified entry is a root.
     * @param node the <code>PermissionDataNode</code> instance for which the parent is to be obtained;
     * may not be null
     * @return the parent entry, or null if there is no parent
     */
    public PermissionNode getParent(PermissionNode node) {
        PermissionDataNodeImpl theNode = this.assertPermissionDataNode(node);
        return (PermissionNode) theNode.getParent();
    }

    /**
     * Obtain the set of entries that are considered the children of the specified
     * <code>PermissionDataNode</code>.
     * @param parent the <code>PermissionDataNode</code> instance for which the child entries
     * are to be obtained; may not be null
     * @return The list of <code>PermissionDataNode</code> instances that are considered
     * the children of the specified entry; never null but possibly empty
     */
    public List getChildren(PermissionNode parent) {
        PermissionDataNodeImpl theParent = this.assertPermissionDataNode(parent);
        return theParent.getChildren();
    }

    /**
     * Determine whether the given <code>descendant</code> is a descendant of the given
     * <code>ancestor</code>.<br></br>
     * This method will check <i>all</i> ancesters of the descendant, even if they are marked hidden.
     * @param ancestor The node to check to see if it is an ancestor of the <code>descendant</code>.
     * @param descendant The node to check to see if it is a descendant <code>ancestor</code>.
     * @return <code>true</code> if <code>ancestor</code> is the ancestor of <code>descendant</code>.
     */
    public boolean isAncestorOf( PermissionNode ancestor, PermissionNode descendant ) {
        PermissionDataNodeImpl theAncestor = this.assertPermissionDataNode(ancestor);
        PermissionDataNodeImpl theDescendant = this.assertPermissionDataNode(descendant);
        PermissionDataNodeImpl parent = (PermissionDataNodeImpl) theDescendant.getParent();

        while ( parent != null ) {
            if ( parent.equals(theAncestor) ) {
                return true;
            }
            parent = (PermissionDataNodeImpl) parent.getParent();
        }
        return false;
    }

    /**
     * Determine whether the given <code>descendant</code> is a descendant of the given
     * <code>ancestor</code>.<br></br>
     * This method will check <i>all</i> descendants of the ancester, even if they are marked hidden.
     * @param ancestor The node to check to see if it is an ancestor of the <code>descendant</code>.
     * @param descendant The node to check to see if it is a descendant <code>ancestor</code>.
     * @return <code>true</code> if <code>ancestor</code> is the ancestor of <code>descendant</code>.
     */
    public boolean isDescendantOf( PermissionNode ancestor, PermissionNode descendant ) {
        PermissionDataNodeImpl theAncester = this.assertPermissionDataNode(ancestor);
        PermissionDataNodeImpl theDescendant = this.assertPermissionDataNode(descendant);

        List nodes = new ArrayList();
        // 1st true => unhide all nodes
        // false => don't include root
        fillNodeList(theAncester, true, false, nodes);
        Iterator nodeItr = nodes.iterator();

        while ( nodeItr.hasNext() ) {
            PermissionDataNodeImpl node = (PermissionDataNodeImpl) nodeItr.next();
            if ( node.equals(theDescendant) ) {
                return true;
            }
        }
        return false;
    }

    /**
     * Set the allowed <code>AuthorizationActions</code> for the subtree rooted at this node (inclusive).
     * @param entry The node to mark or unmark.
     * @param actions The actions to allow on this node and its descendants.
     */
    public void setBranchAllowedActions( PermissionNode startingPoint, AuthorizationActions actions ) {
        PermissionDataNodeImpl root = this.assertPermissionDataNode(startingPoint);
        List nodes = new ArrayList();
        // 1st true => unhide all nodes
        // 2nd true => include root
        fillNodeList(root, true, true, nodes);
        Iterator nodeItr = nodes.iterator();

        while ( nodeItr.hasNext() ) {
            PermissionDataNodeImpl node = (PermissionDataNodeImpl) nodeItr.next();
            node.setAllowedActions(actions);
        }
    }

    /**
     * Set the subtree rooted at this node as hidden (inclusive).
     * @see setShowHidden
     * @param entry The node to mark or unmark.
     */
    public void setBranchHidden( PermissionNode startingPoint ) {
        PermissionDataNodeImpl root = this.assertPermissionDataNode(startingPoint);
        List nodes = new ArrayList();
        // 1st true => unhide all nodes
        // 2nd true => include root
        fillNodeList(root, true, true, nodes);
        Iterator nodeItr = nodes.iterator();

        while ( nodeItr.hasNext() ) {
            PermissionDataNodeImpl node = (PermissionDataNodeImpl) nodeItr.next();
            node.setHidden(true);
        }
    }

    /**
     * Reset all tree nodes to their original <code>AuthorizationActions</code> values.
     */
    public void resetTree() {
        List nodes = new ArrayList();
        // 1st true => unhide all nodes
        // 2nd true => include root
        fillNodeList(this.fakeRoot, true, true, nodes);
        Iterator nodeItr = nodes.iterator();

        while ( nodeItr.hasNext() ) {
            PermissionDataNodeImpl node = (PermissionDataNodeImpl) nodeItr.next();
            node.resetNode();
        }
    }

    /**
     * Obtain all the <i>marked</i> <code>PermissionDataNode</code>s in the tree. Note that the
     * nodes are not nessesarily in tree form. They are just a collection of nodes.<br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @return The <code>Set</code> of all marked nodes in the tree.
     */
    public Set getMarked() {
        // Will only get hidden nodes if showHidden == true;
        Iterator nodeItr = this.iterator();
        return getMarkedNodes(nodeItr);
    }

    /**
     * Obtain all the <i>marked</i> <code>PermissionDataNode</code>s in the tree <i>under</i> the given
     * node. Note that the nodes are not nessesarily in tree form. They are just a
     * collection of nodes.
     * <br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @return The <code>Set</code> of all marked nodes in the tree below <code>startingPoint</code>.
     */
    public Set getMarkedDescendants( PermissionNode startingPoint ) {
        PermissionDataNodeImpl root = this.assertPermissionDataNode(startingPoint);

        List nodes = new ArrayList();
        // Will only get hidden nodes if showHidden == true;
        // false => don't include root
        fillNodeList(root, showHidden, false, nodes);
        return getMarkedNodes(nodes.iterator());
    }

    /**
     * Obtain all the <code>PermissionDataNode</code>s in the tree <i>under</i> the given
     * node. Note that the nodes are not nessesarily in tree form. They are just a
     * collection of nodes.
     * <br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @param startingPoint The point in the tree to start the search for descendants.
     * @return The <code>List</code> of all nodes in the tree below <code>startingPoint</code>.
     */
    public List getDescendants( PermissionNode startingPoint ) {
        PermissionDataNodeImpl root = this.assertPermissionDataNode(startingPoint);

        List nodes = new ArrayList();
        // Will only get hidden nodes if showHidden == true;
        // false => don't include root
        fillNodeList(root, showHidden, false, nodes);
        List descendants = new ArrayList();
        Iterator nodeItr = nodes.iterator();
        while ( nodeItr.hasNext() ) {
            descendants.add(nodeItr.next());
        }
        return descendants;
    }

    /**
     * Obtain all the <code>PermissionDataNode</code>s in the tree <i>under</i> the given
     * node. Note that the nodes are not nessesarily in tree form. They are just a
     * collection of nodes.
     * <br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @param startingPoint The point in the tree to start the search for descendants.
     * @param actions Get only those descendants that possess the given <code>AuthorizationActions</code>.
     * @param strict If <code>true</code>, get only those descendants with actions <i>equal</i> to
     * the given actions.
     * @return The <code>List</code> of all nodes in the tree below <code>startingPoint</code> that
     * possess the given actions.
     */
    public List getDescendantsWithActions( PermissionNode startingPoint,
                                           AuthorizationActions actions,
                                           boolean strict) {
        PermissionDataNodeImpl root = this.assertPermissionDataNode(startingPoint);
        int commonActionValue = actions.getValue();

        List nodes = new ArrayList();
        // Will only get hidden nodes if showHidden == true;
        // false => don't include root
        fillNodeList(root, showHidden, false, nodes);
        List descendants = new ArrayList();
        Iterator nodeItr = nodes.iterator();
        while ( nodeItr.hasNext() ) {
            PermissionDataNodeImpl aNode = (PermissionDataNodeImpl) nodeItr.next();
            if ( strict ) {
                if ( aNode.getActions().equals(actions) ) {
                    descendants.add(aNode);
                }
            } else {
                if ( PermissionDataNodeImpl.shareActions(aNode.getActions().getValue(), commonActionValue) ) {
                    descendants.add(aNode);
                }
            }
        }
        return descendants;
    }

    /**
     * Obtain all the <i>modified</i> <code>PermissionDataNode</code>s in the tree.<br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @return The <code>List</code> of all modified nodes in the tree.
     */
    public List getModified() {
        // Will only get hidden nodes if showHidden == true;
        Iterator nodeItr = this.iterator();
        return getModifiedNodes(nodeItr);
    }

    /**
     * Obtain the <i>modified</i> <code>PermissionDataNode</code>s closest to the root in the tree.
     * <br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @return The <code>List</code> of all top level modified nodes in the tree.
     */
    public List getModifiedBreadthFirst() {
        // Will only get hidden nodes if showHidden == true;
        Iterator nodeItr = this.breadthFirstIterator();
        return getModifiedNodes(nodeItr);
    }

    /**
     * Obtain all the <i>modified</i> <code>PermissionDataNode</code>s in the tree <i>under</i> the
     * given node. Note that the nodes are not nessesarily in tree form.
     * <br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @return The <code>List</code> of all modified nodes in the tree below <code>startingPoint</code>.
     */
    public List getModifiedDescendants( PermissionNode startingPoint ) {
        PermissionDataNodeImpl root = this.assertPermissionDataNode(startingPoint);

        List nodes = new ArrayList();
        // Will only get hidden nodes if showHidden == true;
        // false => don't include root
        fillNodeList(root, showHidden, false, nodes);
        return getModifiedNodes(nodes.iterator());
    }

    /**
     * Obtain all the <i>unmodified</i> <code>PermissionDataNode</code>s in the tree <i>under</i> the
     * given node. Note that the nodes are not nessesarily in tree form.
     * <br></br>
     * This method will get hidden nodes if <code>showHidden == true</code>.
     * @return The <code>List</code> of all unmodified nodes in the tree below <code>startingPoint</code>.
     */
    public List getUnModifiedDescendants( PermissionNode startingPoint ) {
        PermissionDataNodeImpl root = this.assertPermissionDataNode(startingPoint);

        List nodes = new ArrayList();
        // Will only get hidden nodes if showHidden == true;
        // false => don't include root
        fillNodeList(root, showHidden, false, nodes);
        return getModifiedNodes(nodes.iterator());
    }

    /**
     * Determine whether all descendants of the given node share the <i>exact same</i> actions as the
     * given node. This determination is independant of the state of <code>showHidden</code>.
     * @param startingPoint The root of the subtree to check.
     * @returns <code>true</code> if <i>all</i> of the given node's descendants share the
     * <i>exact same</i> actions, <code>false</code> otherwise.
     */
    public boolean allDescendantsShareActions( PermissionNode startingPoint ) {
        PermissionDataNodeImpl root = this.assertPermissionDataNode(startingPoint);

        List nodes = new ArrayList();
        // true => unhide all nodes
        // false => don't include root
        fillNodeList(root, true, false, nodes);
        Iterator nodeItr = nodes.iterator();

        while ( nodeItr.hasNext() ) {
            PermissionDataNodeImpl descendant = (PermissionDataNodeImpl) nodeItr.next();
            if ( ! descendant.actionsAreEqual(root) ) {
                return false;
            }
        }

        return true;
    }

    /**
     * Set the permissions on the node containing each permission's resource.
     * @param permissions The <code>Collection</code> of <code>BasicAuthorizationPermission</code>s to set
     * (each contains the the resource and an AuthorizationAction.)
     * @return The <code>Collection</code> of <code>DataNodeExceptions</code>s each containing
     * a resource name that was not found in the tree.
     */
    public Collection setPermissions(Collection permissions) {
        // May contain resource names that were not found
        List nodeExceptions = new ArrayList();

        // Init the Map of resource name -> PermissionDataNode
        if ( permissions.size() > 0 ) {
            this.initResourceMap();
        }

        Iterator permItr = permissions.iterator();
        while ( permItr.hasNext() ) {
            AuthorizationPermission aPerm = (AuthorizationPermission) permItr.next();
// DEBUG:
//System.out.println("\n *** setPermissions: Perm: <" + aPerm.getResourceName() + "-" + aPerm.getActions() +">");
            boolean nodeFound = false;

            String resource = aPerm.getResourceName();
            if ( BasicAuthorizationPermission.isRecursiveResource(resource) ) {
                // If this perm is recursive, remove recursion so it can be found in node map
                resource = BasicAuthorizationPermission.removeRecursion(resource);
// DEBUG:
//System.out.println(" *** setPermissions: Perm is RECURSIVE - New resource: " + resource);
            }

            PermissionDataNodeImpl aNode = (PermissionDataNodeImpl) this.resourceLookUp.get(resource);
            if ( aNode != null && aNode.getResourceName().equals(resource) ) {
                nodeFound = true;
                AuthorizationActions actions = aPerm.getActions();
// DEBUG:
//System.out.println(" *** setPermissions: Found node. Setting actions: <" + actions + "> on node resource " + aNode.getResourceName());
                aNode.setInitialActions(actions);
            }
            if ( ! nodeFound ) {
// DEBUG:
//System.out.println(" *** setPermissions: FAILED to find node for: " + resource);
                nodeExceptions.add( new PermissionNodeNotFoundException(AdminMessages.ADMIN_0042,
                		SecurityPlugin.Util.getString(AdminMessages.ADMIN_0042 ,resource) ));
            }
            // Reset if found
            nodeFound = false;
        }

// DEBUG:
//System.out.println(" *** setPermissions: Done setting all perms.");
//System.out.println("================================================================================");
        return nodeExceptions;
    }

//==================================================================================================
// Private methods
//==================================================================================================

    /**
     * Recursive method prepares tree nodes as an ordered depth-first List starting from <code>root</code>.
     * @param root The root of the (sub)tree from which to start.
     * @param showHidden Determines whether or not to include nodes marked hidden.
     * @param includeRoot Determines whether or not to include the given root.
     * @param nodes <strong>Modified</strong> - nodes of subtree are added in depth-first manner.
     */
    synchronized static final void fillNodeList( PermissionDataNodeImpl root,
                                                         boolean showHidden,
                                                         boolean includeRoot,
                                                         List nodes ) {
        if ( includeRoot ) {
            if ( ! root.isHidden() || showHidden == true ) {
                nodes.add(root);
            }
        }
        Iterator children = root.getChildren().iterator();
        while ( children.hasNext() ) {
            // true => include root (now child) in succesive calls
            fillNodeList((PermissionDataNodeImpl) children.next(), showHidden, true, nodes);
        }
    }

    /**
     * Recursive method prepares tree nodes as an ordered breadth-first List starting from <code>root</code>.
     * @param siblings The list of nodes from which to start the search.
     * @param showHidden Determines whether or not to include nodes marked hidden.
     * @param includeRoot Determines whether or not to include the given root.
     * @param nodes <strong>Modified</strong> - nodes of subtree are added in breadth-first manner.
     */
    synchronized static final List fillNodeListBreadthFirst( PermissionDataNodeImpl root,
                                                             boolean showHidden,
                                                             boolean includeRoot ) {
        List nodes = new ArrayList();
        if ( includeRoot ) {
            if ( ! root.isHidden() || showHidden == true ) {
                nodes.add(root);
            }
        }

        if ( nodes.size() == 0 ) {
            List children = root.getChildren();
            Iterator childItr = children.iterator();
            while ( childItr.hasNext() ) {
                PermissionDataNodeImpl child = (PermissionDataNodeImpl) childItr.next();
                if ( ! child.isHidden() || showHidden == true ) {
                    nodes.add(child);
                }
            }
        }

        int index = 0;
        for ( ; index < nodes.size(); index++ ) {
            nodes.addAll( ((PermissionDataNodeImpl) nodes.get(index)).getChildren() );
        }

        return nodes;
    }

    /**
     * Obtain all the <i>marked</i> <code>PermissionDataNode</code>s from the given node Iterator.
     * @param nodeItr The nodes for concideration.
     * @return The <code>Set</code> of all marked nodes in the Iterator.
     */
    private synchronized static final Set getMarkedNodes(Iterator nodeItr) {
        Set markedNodes = new HashSet();
        while ( nodeItr.hasNext() ) {
            PermissionDataNodeImpl node = (PermissionDataNodeImpl) nodeItr.next();
            if ( node.isMarked() ) {
                markedNodes.add(node);
            }
        }
        return markedNodes;
    }

    /**
     * Obtain all the <i>modified</i> <code>PermissionDataNode</code>s from the given node Iterator.
     * @param nodeItr The nodes for concideration.
     * @return The <code>List</code> of all modified nodes in the Iterator.
     */
    private synchronized static final List getModifiedNodes(Iterator nodeItr) {
        List modifiedNodes = new ArrayList();
        while ( nodeItr.hasNext() ) {
            PermissionDataNodeImpl node = (PermissionDataNodeImpl) nodeItr.next();
            if ( node.isModified() ) {
                modifiedNodes.add(node);
            }
        }
        return modifiedNodes;
    }

//    /**
//     * Obtain all the <i>unmodified</i> <code>PermissionDataNode</code>s from the given node Iterator.
//     * @param nodeItr The nodes for concideration.
//     * @return The <code>List</code> of all unmodified nodes in the Iterator.
//     */
//    private synchronized static final List getUnModifiedNodes(Iterator nodeItr) {
//        List unmodifiedNodes = new ArrayList();
//        while ( nodeItr.hasNext() ) {
//            PermissionDataNodeImpl node = (PermissionDataNodeImpl) nodeItr.next();
//            if ( ! node.isModified() ) {
//                unmodifiedNodes.add(node);
//            }
//        }
//        return unmodifiedNodes;
//    }

    /**
     * Init the resourceName -> PermisionDataNode Map
     */
    private void initResourceMap() {
        this.resourceLookUp = new HashMap();
        // Get all the nodes
        List nodes = new ArrayList();
        // true => unhide all nodes
        // false => do not include the fake root
        fillNodeList(fakeRoot, true, false, nodes);
        Iterator nodeItr = nodes.iterator();
        while ( nodeItr.hasNext() ) {
            PermissionDataNodeImpl aNode = (PermissionDataNodeImpl) nodeItr.next();
            this.resourceLookUp.put(aNode.getResourceName(), aNode);
        }
    }

}
