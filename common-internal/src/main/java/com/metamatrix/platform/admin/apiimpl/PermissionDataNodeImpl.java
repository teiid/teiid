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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.admin.AdminMessages;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.tree.basic.BasicTreeNode;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.core.id.ObjectID;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.platform.admin.api.PermissionDataNode;
import com.metamatrix.platform.admin.api.PermissionDataNodeDefinition;
import com.metamatrix.platform.admin.api.PermissionDataNodeTreeView;
import com.metamatrix.platform.admin.api.PermissionNode;
import com.metamatrix.platform.admin.api.exception.PermissionNodeNotActionableException;
import com.metamatrix.platform.security.api.AuthorizationActions;
import com.metamatrix.platform.security.api.SecurityPlugin;
import com.metamatrix.platform.security.api.StandardAuthorizationActions;

/**
 * Defines a tree node for displaying entitlements.
 */
public class PermissionDataNodeImpl extends BasicTreeNode implements PermissionDataNode, Serializable, Comparable {

    private int actionsCreatedWith;
    private int descendantActions;
    private AuthorizationActions actions;
    private AuthorizationActions allowedActions;
    private boolean isHidden;
    private boolean isPhysical;
    private int hashCode;

    /**
     * Construct a new instance by specifying the parent that owns this entity,
     * the name of the instance, the type of the instance and the global UID
     * for the instance.
     * <b>Note:</b> this constructor does <i>NOT</i> verify that the name is valid within
     * the parent's namespace.
     * @param parent the parent that is considered the owning namespace for this
     * instance; may be null if the new instance is to be the root
     * @param allowedActions Contain the only actions that will be allowed on this node.
     * @param nodeDefinition The "<i>type</i>" of this treeNode which contains the "<i>subtype</i>"
     * of the <code>PermissionDataNode</code>: {@link PermissionDataNodeDefinition.TYPE PermissionDataNodeDefinition.TYPE}
     * @param nodeIsPhysical Is this node physical are virtual.
     * @param guid the globally-unique identifier for this instance; may not be null
     */
    public PermissionDataNodeImpl(BasicTreeNode parent,
                                    AuthorizationActions allowedActions,
                                    PermissionDataNodeDefinition nodeDefinition,
                                    boolean nodeIsPhysical,
                                    ObjectID guid) {
        super(parent, nodeDefinition.getDisplayName(), nodeDefinition, guid);
        this.allowedActions = allowedActions;
        this.descendantActions = StandardAuthorizationActions.NONE_VALUE;
        this.actions = StandardAuthorizationActions.NONE;
        this.actionsCreatedWith = this.actions.getValue();
        this.isHidden = false;
        this.isPhysical = nodeIsPhysical;
        this.hashCode = nodeDefinition.getName().hashCode();
        // BasicTreeNode is set to modified when created! Unset it.
        this.setModified(false, false);
    }

    /**
     * Get the <i>type</i> of this <code>PermissionDataNode</code>.
     * <br>This method returns the <i>int</i> type of this node which
     * coresponds to {@link PermissionDataNodeDefinition.TYPE PermissionDataNodeDefinition.TYPE }.</br>
     * @return The type of this <code>PermisionDataNode</code>.
     */
    public int getDataNodeType() {
        return ((PermissionDataNodeDefinition) getType()).getType();
    }

    /**
     * Display name for node.
     * @return The name of the data node for display.
     */
    public String getDisplayName() {
        return this.getName();
    }

    /**
     * Resource name for node (the DataNodeID).
     * @return The resource name of the data node.
     */
    public String getResourceName() {
        return getType().getName();
    }

    /**
     * Does this node belong to a physical model?
     * @return <code>true</code> if this node is part of a physical model.
     */
    public boolean isPhysical() {
        return this.isPhysical;
    }

    /**
     * Does this node have <i>any</i> permission associated with it?<br></br>
     * <strong>Note: if <code>hasPermission()</code> returns <code>false</code>,
     * there will be no <code>AuthorizatinPoicyID</code> associated with this node
     * and <code>getPolicyID()</code> will return <code>null</code></strong>.
     * @return True, if this node has an <code>Action</code> other than <code>NONE</code>.
     */
    public boolean hasPermission() {
        return ! this.actions.equals(StandardAuthorizationActions.NONE);
    }

    /**
     * The node's unique ID.
     * @return The unique ID for the node.
     */
    public ObjectID getUUID() {
        return this.getGlobalUID();
    }

    /**
     * The <code>AuthorizationActions</code> labels currently on this data node.
     * @see com.metatamatrix.platform.security.api.StandardAuthorizationActions
     * @return The Array <String> of one or more of {"None", "Create",
     * "Read", "Update", "Delete"}.
     */
    public String[] getActionLabels() {
        return actions.getLabels();
    }

    /**
     * The <code>AuthorizationActions</code> currently on this data node.
     * @see com.metatamatrix.platform.security.api.StandardAuthorizationActions
     * @return The actions allowed on this data node.
     */
    public AuthorizationActions getActions() {
        return actions;
    }

    /**
     * The <code>AuthorizationActions</code> allowed on this data node.
     * @see com.metatamatrix.platform.security.api.StandardAuthorizationActions
     * @return The actions allowed on this data node.
     */
    public AuthorizationActions getAllowedActions() {
        return this.allowedActions;
    }

    /**
     * Set the <code>AuthorizationActions</code> on this data node.
     * @param actions The the actions to set on this data node.
     * @throws PermissionNodeNotActionableException If attempt is made to set actions on a node that can't
     * accept <i>any</code> actions.
     */
    public void setActions(AuthorizationActions actions) throws PermissionNodeNotActionableException {
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,
            "setActions(AuthoriztionActions): setting <" + actions + "> on <" + getType().getName() + //$NON-NLS-1$ //$NON-NLS-2$
            ">: Allowed actions: <" + this.allowedActions + ">"); //$NON-NLS-1$ //$NON-NLS-2$
        if ( ! this.allowedActions.implies(actions) ) {
            throw new PermissionNodeNotActionableException(AdminMessages.ADMIN_0040,
            		SecurityPlugin.Util.getString(AdminMessages.ADMIN_0040, getType().getName(), actions.toString()));
        }

        this.actions = actions;
        this.setModified(true, false);
        // Set appropriate permission state, reveal state to ancestery and set modified.
// FIXME: Not propagating actions on server for SP1
//        setPermissionStateAndPropagate(actions, true);
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,
            "setActions(AuthorizationActions): set <" + this.actions + "> on <" + getType().getName() + ">"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    /**
     * Set the allowed <code>AuthorizationActions</code> on this data node.
     * <br></br>
     * @param actions The the actions to set on this data node.
     * @throws PermissionNodeNotActionableException If attempt is made to set actions on a node that can't
     * accept <i>any</code> actions.
     */
    public void setActions(int actions) throws PermissionNodeNotActionableException {
        // Validity checking performed here
        AuthorizationActions newActions = StandardAuthorizationActions.getAuthorizationActions(actions);
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,
            "setActions(int): setting <" + newActions + "> on <" + getType().getName() + //$NON-NLS-1$ //$NON-NLS-2$
            ">: Allowed actions: <" + this.allowedActions + ">"); //$NON-NLS-1$ //$NON-NLS-2$
        if ( ! this.allowedActions.implies(newActions) ) {
			throw new PermissionNodeNotActionableException(AdminMessages.ADMIN_0040,
					SecurityPlugin.Util.getString(AdminMessages.ADMIN_0040, getType().getName(), newActions.toString()));
        }
        this.actions = newActions;
        this.setModified(true, false);
        // Set appropriate permission state, reveal state to ancestery and set modified.
// FIXME: Not propagating actions on server for SP1
//        setPermissionStateAndPropagate(actions, true);
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,
            "setActions(int): set <" + this.actions + "> on <" + getType().getName() + ">"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    /**
     * Set the <code>AuthorizationActions</code> on this data node.<br></br>
     * Note that only the subset of allowed actions will be taken from the given argument.
     * @param actions The the actions to set on this data node.
     */
    private void privlegedSetActions(AuthorizationActions actions) {
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,
            "privlegedSetActions(AuthoriztionActions): setting <" + actions + "> on <" + getType().getName() + //$NON-NLS-1$ //$NON-NLS-2$
            ">: Allowed actions: <" + this.allowedActions + ">"); //$NON-NLS-1$ //$NON-NLS-2$
        // Set the actions on the node to the given actions masked by the allowed actions.
         this.actions =
            StandardAuthorizationActions.getAuthorizationActions(actions.getValue() & this.allowedActions.getValue());
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,
            "privlegedSetActions(AuthoriztionActions): Actions now: <" + this.actions + "> on <" + getType().getName() + ">"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    /**
     * Set the <code>AuthorizationActions</code> on this data node and set the actions created at
     * the same time.  This will insure that the node does not look modified.<br></br>
     * <strong>This package-level method should only be used by the tree view when creating the tree.</strong>
     * Note that only the subset of allowed actions will be taken from the given argument.
     * @param actions The the actions to set on this data node.
     */
    void setInitialActions(AuthorizationActions initialActions) {
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,
            "setInitialActions(AuthoriztionActions): setting <" + initialActions + "> on <" + getType().getName() + //$NON-NLS-1$ //$NON-NLS-2$
            ">: Allowed actions: <" + this.allowedActions + ">"); //$NON-NLS-1$ //$NON-NLS-2$
        this.actionsCreatedWith = initialActions.getValue();

        // Set the actions on the node to any it had before (becuase of propagation from others)
        // adding the initialActions of the permission.
        this.actions = initialActions;

        // Propagate state and set modified
// FIXME: Not propagating actions on server for SP1
//        this.setPermissionStateAndPropagate(actions, false);
    }

    /**
     * Get the original actions this node was created with.
     * @return The actions this node was created with.
     */
    public AuthorizationActions getOriginalActions() {
        return StandardAuthorizationActions.getAuthorizationActions(this.actionsCreatedWith);
    }

    /**
     * Set the allowed <code>AuthorizationActions</code> on this data node.
     * @param actions The the actions that can be applied to this data node.
     */
    public void setAllowedActions(AuthorizationActions actions) {
        this.allowedActions = actions;
    }

    /**
     * Reset this node to its original <code>AuthorizationActions</code>.
     */
    void resetNode() {
        this.actions = StandardAuthorizationActions.getAuthorizationActions(this.actionsCreatedWith);
        this.setModified(false, false);
    }

    /**
     * Determine if this node's <code>AuthorizationActions</code> are equal to the given actions. The actions
     * are considered equal if number of actions are the same and all corresponding pairs of actions
     * are equal.
     * @param actions The <code>AuthorizationActions</code> to compare with this node's actions.
     * @return true if the <code>Actions</code> of this node are equal to the given actions.
     */
    public boolean actionsAreEqual(AuthorizationActions actions) {
        return this.actions.equals(actions);
    }

    /**
     * Determine if this node's <code>AuthorizationActions</code> are equal to the given actions. The actions
     * are considered equal if number of actions are the same and all corresponding pairs of actions
     * are equal.
     * @param actions The Array <String> of one or more of {"None", "Create",
     * "Read", "Update", "Delete"}.
     * @return true if the <code>Actions</code> of this node are equal to the given actions.
     */
    public boolean actionsAreEqual(String[] actions) {
        AuthorizationActions thoseActions = StandardAuthorizationActions.getAuthorizationActions(actions);
        return this.actions.equals(thoseActions);
    }

    /**
     * Determine if this node's <code>AuthorizationActions</code> are equal to the given node's
     * actions. The actions are considered equal if number of actions are the same and all
     * corresponding pairs of actions are equal.
     * @param node The node whose actions to compare with this node's actions.
     * @return true if the <code>Actions</code> of this node are equal to the given node's actions.
     */
    public boolean actionsAreEqual(PermissionNode node) {
        AuthorizationActions thoseActions = node.getActions();
        return this.actions.equals(thoseActions);
    }

    /**
     * Are there <i>any</i> entitled nodes below this point in the tree?
     * @return True, if a decendant is entitled with <i>any</i> AuthorizationActions, False otherwise.
     */
    public boolean isDescendantEnabled() {
        return descendantActions != StandardAuthorizationActions.NONE_VALUE;
    }

    /**
     * Does this node have any descendants enabled for the given <code>AuthorizationActions</code>?
     * @param actions The actions of interest.
     * @return True, if a decendant of this node is entitled with the given actions, False otherwise.
     */
    public boolean isDescendantEnabledFor(AuthorizationActions actions) {
        return (descendantActions & actions.getValue()) != 0;
    }

    /**
     * Do <i>all</> children of this node share the given <code>AuthorizationActions</code>?
     * @param actions The actions of interest.
     * @return True, if <i>all</> children of this node share the given actions, False otherwise.
     */
    public boolean allChildrenEnabledFor(AuthorizationActions actions) {
        int actionsInQuestion = actions.getValue();
        // getChildren() returns an unmodifiable collection
        List children = this.getChildren();
        Iterator childItr = children.iterator();
        while ( childItr.hasNext() ) {
            PermissionDataNodeImpl child = (PermissionDataNodeImpl) childItr.next();
            if ( (actionsInQuestion & child.getActions().getValue()) != actionsInQuestion ) {
                return false;
            }
        }
        return true;
    }

    /**
     * A node is recursive if its actions are shared by its descendants' actions.
     * @return <code>true</code> if this node is recursive.
     */
    public boolean isRecursive() {
        return ( ! isLeafNode() &&
                 (this.actions.equals( StandardAuthorizationActions.getCommonActions(this.actions.getValue(),
                                       getActionsCommonToChildren(this)))) );
    }

    /**
     * Check whether or not this node is hidden from the <code>PermissionDataNodeTreeView</code>. The
     * default is <code>false</code>, the node is not hidden.
     * @see PermissionDataNodeTreeView#setShowHidden
     * @return <code>true</code> if this node may be hidden from the view.
     */
    public boolean isHidden() {
        return this.isHidden;
    }

    /**
     * Set whether or not this node is hidden from the <code>PermissionDataNodeTreeView</code>. The
     * default is <code>false</code>, the node is not hidden.
     * @see PermissionDataNodeTreeView#setShowHidden
     * @param isHidden If <code>true</code>, this node may be hidden from the view.
     */
    public void setHidden(boolean isHidden) {
        this.isHidden = isHidden;
    }

//    /**
//     * Do <i>all</i> of this node's children share the same <code>AuthorizationActions</code> as
//     * this node?.  If this node is a leaf or has no actions, <code>false</code> is returned.
//     * @return <code>true</code> if this node's children have the same actions as it does.
//     */
//    private boolean childrenShareActions() {
//        if ( ! this.hasPermission() || this.getChildCount() == 0 ) {
//            return false;
//        }
//        Iterator childItr = this.getChildren().iterator();
//        while ( childItr.hasNext() ) {
//            PermissionDataNodeImpl child = (PermissionDataNodeImpl) childItr.next();
//            if ( ! child.hasPermission() || ! this.actions.equals(child.getActions()) ) {
//                return false;
//            }
//        }
//        return true;
//    }

    /**
     * Is this node a leaf?
     * @return <code>true</code>, if this node has no children.
     */
    public boolean isLeafNode() {
        return this.getChildCount() == 0;
    }

    /**
     * Is this node a group (has only one level of children)?
     * @return <code>true</code>, if this node has only on level of children.
     */
    public boolean isGroupNode() {
        List nodes = new ArrayList();
        // true => unhide all nodes
        // false => include this node
        PermissionDataNodeTreeViewImpl.fillNodeList(this, true, false, nodes);
        if ( nodes.size() == 0 ) {
            // This node has no children - leaf
            return false;
        }
        Iterator nodeItr = nodes.iterator();
        while ( nodeItr.hasNext() ) {
            PermissionDataNodeImpl aChild = (PermissionDataNodeImpl) nodeItr.next();
            if ( aChild.getChildCount() > 0 ) {
                // To be a group, all of this node's children can have no descendants
                return false;
            }
        }
        return true;
    }

    /**
     * Get the actions allowed by all descendants.
     */
    public AuthorizationActions getDescendantActions() {
        return StandardAuthorizationActions.getAuthorizationActions(this.descendantActions);
    }

    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if ( this == obj ) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        boolean areEqual = false;
        if ( obj instanceof PermissionDataNodeImpl ) {
            PermissionDataNodeImpl that = (PermissionDataNodeImpl) obj;
                // Must always have resourceNames
                areEqual = getType().getName().equals( that.getResourceName() );
		}

        return areEqual;
    }
    /**
     * Compares this object to another. If the specified object is not an instance of
     * the <code>PermissionDataNodeImpl</code> class, then this method throws a
     * ClassCastException (as instances are comparable only to instances of the same
     * class).
     * Note:  this method <i>is</i> consistent with <code>equals()</code>, meaning
     * that <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * @param obj the object that this instance is to be compared to; may not be null.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        PermissionDataNodeImpl that = (PermissionDataNodeImpl) obj;     // May throw ClassCastException
        if(obj == null){
            Assertion.isNotNull(obj,SecurityPlugin.Util.getString(AdminMessages.ADMIN_0039));
        }
        
        if ( obj == this ) {
            return 0;
        }

        return compare(this, that);
    }

    /**
     * Utility method to compare two PermissionDataNodeImpl instances.  Returns a negative integer, zero,
     * or a positive integer as this object is less than, equal to, or greater than
     * the specified object. <p>
     *
     * This method assumes that all type-checking has already been performed. <p>
     *
     * @param obj1 the first node to be compared
     * @param obj2 the second node to be compared
     * @return -1, 0, +1 based on whether obj1 is less than, equal to, or
     *         greater than obj2
     */
    static public final int compare(PermissionDataNodeImpl obj1, PermissionDataNodeImpl obj2) {
        int resourceDiff = obj1.getResourceName().compareTo(obj2.getResourceName());
        if(resourceDiff != 0){
            return resourceDiff;
        }

        return obj1.getUUID().compareTo(obj2.getUUID());
    }

    /**
     * Override hashCode() to be consistant with overriden equals().
     */
    public int hashCode() {
        return this.hashCode;
    }

    /**
     * Return stringafied representation of the object.
     */
    public String toString() {
        return getType().getName() + " " + this.actions; //$NON-NLS-1$
    }

    /**
     * Return stringafied representation of the object.
     */
    public String printDebug() {
        StringBuffer buf = new StringBuffer();
        buf.append("\nResource: " + getType().getName()); //$NON-NLS-1$
//        buf.append("\n    UUID: " + this.getGlobalUID());
        buf.append("\n    Actions: " + this.actions); //$NON-NLS-1$
        buf.append("\n    Has Permission: " + (this.hasPermission() == true ? "YES" : "NO")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        buf.append("\n    Descendant Enabled: " + (this.isDescendantEnabled() == true ? "ENABLED-" + StandardAuthorizationActions.getAuthorizationActions(this.descendantActions) : "DISABLED") ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        buf.append("\n    Is Leaf Node: " + (this.isLeafNode() == true ? "YES" : "NO")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        buf.append("\n    Is Hidden: " + (this.isHidden() == true ? "YES" : "NO")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        buf.append("\n    Modified: " + (this.isModified() == true ? "YES" : "NO")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        return buf.toString();
    }

//==================================================================================================
// Helper methods
//==================================================================================================

    /**
     * Sets the state of this node according to the given actions.  The state is propagated to all
     * descendants recursively.  When a leaf is finally reached, the "descedantEnabled" state is
     * propagated up to all ancesters.
     * @param actions The new actions to set on this node and its descendants.
     * @param modified Whether or not to set each descendant modified.
     */
    void setPermissionStateAndPropagate(AuthorizationActions actions, boolean modified) {
// DEBUG
//System.out.println("\n *** setPermissionStateAndPropagate: Setting actions: <" + actions + "> on <" + this.toString() + ">");
        this.privlegedSetActions(actions);
        this.setModified(modified, false);
// DEBUG
//System.out.println(" *** setPermissionStateAndPropagate: Propagating effected node: <" + this.toString() + "> Modified: <" + this.isModified() + ">");
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,"setPermissionStateAndPropagate: Propagating effected node: <" + this.toString() + "> Modified: <" + this.isModified() + ">"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        propagateActionsToChildren(actions, modified);
// DEBUG
//System.out.println(" *** setPermissionStateAndPropagate: Done propagating node: <" + this.toString() + ">\n");
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,"setPermissionStateAndPropagate: Done propagating node: <" + this.toString() + ">\n"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Set the <code>AuthorizationActions</code> on all descendants below this node.
     * When the last leaf node is encountered, propagateDescendantEnabled is called on it so that
     * ancesters are aware of the actions of their descendants.
     * @param actions The actions to propagate.
     * @param modified Set node modified to this value.
     */
    private void propagateActionsToChildren(AuthorizationActions actions, boolean modified) {
        List nodes = new ArrayList();
        // true => unhide all nodes
        // false => dont't include root
        PermissionDataNodeTreeViewImpl.fillNodeList(this, true, false, nodes);
        Iterator nodeItr = nodes.iterator();

        // If we're at a leaf, this node will be used to propagateDescendantEnabled
        PermissionDataNodeImpl node = this;
        // Propagate actions to children depth first
        while ( nodeItr.hasNext() ) {
            node = (PermissionDataNodeImpl) nodeItr.next();
// DEBUG
//System.out.println("\n *** propActsToChildren: Setting <" + actions + "> on node: <" + node + ">");
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,"propActsToChildren: Setting <" + actions + "> on node: <" + node + ">"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

            AuthorizationActions theActions =
                StandardAuthorizationActions.getAuthorizationActions(
                                                node.getActions().getValue() | actions.getValue());
            node.privlegedSetActions(theActions);
            node.setModified(modified, false);
// DEBUG
//System.out.println(" *** propActsToChildren: Set <" + node.getActions() + "> on node: <" + node + "> Modified: <" + node.isModified() + ">");
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,"propActsToChildren: Set <" + node.getActions() + "> on node: <" + node + "> Modified: <" + node.isModified() + ">"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        }
// DEBUG
//System.out.println(" *** propActsToChildren: Now setting descendant enabled with node: <" + node + ">");
        LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,"propActsToChildren: Now setting descendant enabled with node: <" + node + ">"); //$NON-NLS-1$ //$NON-NLS-2$
        // Now that all children's actions are set, propagate descendant actions to parents
        // starting with last child
        propagateDescendantEnabled(node);
    }

    /**
     * Propagates the value of <code>enabled</code> to all ancesters of child node.  Terminates when
     * an ancester already has proper <code>descendantEnabled</code> state or runs past the root.
     * @param child The child node from which to propagate the actions.
     */
    private static void propagateDescendantEnabled(PermissionDataNodeImpl child) {
        PermissionDataNodeImpl parent = (PermissionDataNodeImpl) child.getParent();

        // Set descendant and node actions on parent
        // Termination: if parent == null (run out of ancesters)
        while ( parent != null ) {

            // Set descendant actions to actions posessed by all children
            int otherChildrensCompleteActionValues = getAllOtherChildrensActionValues(parent, child);
            parent.descendantActions = otherChildrensCompleteActionValues | child.getActions().getValue();
// DEBUG
//System.out.println("\n *** propagateDescendantEnabled: Set descendant actions for node: <" + parent + ">: <" + parent.getDescendantActions() + ">");
            LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,
                "propagateDescendantEnabled: Descendant actions for node: <" + parent + ">: <" + parent.getDescendantActions() + ">"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

            // Set parent nodes actions to common actions of children. If children have no common
            // actions, use parent node's current actions (don't set it's actions).
            int actionsCommonToChildren = getActionsCommonToChildren(parent);
// DEBUG
//System.out.println(" *** propagateDescendantEnabled: Chilren common actions: <" + StandardAuthorizationActions.getAuthorizationActions(actionsCommonToChildren) + ">");
            if ( actionsCommonToChildren != StandardAuthorizationActions.NONE_VALUE ) {
                AuthorizationActions thisNodesActions =
                    StandardAuthorizationActions.getAuthorizationActions(
                                                    parent.getActions().getValue() | actionsCommonToChildren);
                parent.privlegedSetActions(thisNodesActions);
// DEBUG
//System.out.println(" *** propagateDescendantEnabled: Set actions for node: <" + parent + ">: <" + parent.getActions() + ">");
                LogManager.logDetail(LogConstants.CTX_AUTHORIZATION_ADMIN_API,
                    "Set actions for node: <" + parent + ">: <" + parent.getActions() + ">"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }

            // Get parent of this node
            child = parent;
            parent = (PermissionDataNodeImpl) parent.getParent();
        }
    }

    /**
     * Returns the all actions available to children other than for excluded child.
     */
    private static int getAllOtherChildrensActionValues(PermissionDataNodeImpl parent,
                                                        PermissionDataNodeImpl childToExclude) {
        int siblingActions = StandardAuthorizationActions.NONE_VALUE;
        // getChildren() returns an unmodifiable collection
        List children = new ArrayList(parent.getChildren());
        children.remove(childToExclude);
        Iterator childItr = children.iterator();
        while ( childItr.hasNext() ) {
            PermissionDataNodeImpl child = (PermissionDataNodeImpl) childItr.next();
            siblingActions = siblingActions | child.getActions().getValue();
        }
        return siblingActions;
    }

//    /**
//     * Returns the all actions common to children other than for excluded child.
//     */
//    private static int getOtherChildrensCommonActionValues(PermissionDataNodeImpl parent,
//                                                 PermissionDataNodeImpl childToExclude) {
//        int siblingActions = -1;
//        // getChildren() returns an unmodifiable collection
//        List children = new ArrayList(parent.getChildren());
//        children.remove(childToExclude);
//        Iterator childItr = children.iterator();
//        while ( childItr.hasNext() ) {
//            PermissionDataNodeImpl child = (PermissionDataNodeImpl) childItr.next();
//            if ( siblingActions == -1 ) {
//                siblingActions = child.getActions().getValue();
//            }
//            siblingActions = siblingActions & child.getActions().getValue();
//        }
//        return (siblingActions == -1 ? StandardAuthorizationActions.NONE_VALUE : siblingActions);
//    }

    /**
     * Get the actions common to all this node's children
     * @param parent The node whose children to get the common actions
     */
    private static int getActionsCommonToChildren(PermissionDataNodeImpl parent) {
        int commonChildrenActions = -1;
        List children = parent.getChildren();
        Iterator childItr = children.iterator();
        while ( childItr.hasNext() ) {
            PermissionDataNodeImpl child = (PermissionDataNodeImpl) childItr.next();
            if ( commonChildrenActions == -1 ) {
                commonChildrenActions = child.getActions().getValue();
            }
            commonChildrenActions = commonChildrenActions & child.getActions().getValue();
        }
        return (commonChildrenActions == -1 ? StandardAuthorizationActions.NONE_VALUE : commonChildrenActions);
    }

    /**
     * Are some/all actions shared between 'a' and 'b'?
     * Check to see if any bits set in 'a' are set in 'b'.
     */
    static boolean shareActions(int a, int b) {
// DEBUG:
//System.out.println(" *** shareActions: Sib acts => mod child acts: <" + a + "> <" + b + "> " + ((a & b) != 0 ? "TRUE" : "FALSE"));
        return (a & b) != 0;
    }

//    /**
//     * Return true if all descendants are recursive.
//     * @param recursiveNode The point at which to check for the recursion.
//     */
//    private static boolean descendantsAreRecursive(PermissionDataNodeImpl recursiveRoot) {
//        List nodes = new ArrayList();
//        // 1st false => don't unhide all nodes
//        // 2nd false => don't include recursiveRoot
//        PermissionDataNodeTreeViewImpl.fillNodeList(recursiveRoot, false, false, nodes);
//        Iterator nodeItr = nodes.iterator();
//        while ( nodeItr.hasNext() ) {
//            PermissionDataNodeImpl aDescendant = (PermissionDataNodeImpl) nodeItr.next();
//            if ( aDescendant.isLeafNode() ) {
//                // We're done
//                return true;
//            }
//            if ( ! shareActions(aDescendant.actions.getValue(), aDescendant.descendantActions) ) {
//                // Blow out on first failure
//                return false;
//            }
//        }
//        return true;
//    }
}
