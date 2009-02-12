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

package com.metamatrix.common.actions;

import java.io.Serializable;
import java.util.*;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * This class is the basic and default implementation of the ModificationActionQueue interface.  This
 * implementation has no limit to the number of actions that can be put into the queue, and is
 * fully synchronized for concurrent access.
 */
public class BasicModificationActionQueue implements ModificationActionQueue, Serializable {

    /**
     *@link aggregation
     *      @associates <b>ActionDefinition</b>
     * @supplierCardinality 0..*
     */
    private LinkedList actions = new LinkedList();

    /**
     * Create an instance of the modification action queue.
     */
    public BasicModificationActionQueue() {
    }

    /**
     * Remove all of the modification objects that are in the queue and return them.  Immediately after this
     * method is called, the queue will contain no actions.
     * @return the list of actions, ordered by the time of their creation with the action created last at the end
     * of the list; may be an empty List if there are no actions, but null
     * will not be returned
     */
    public synchronized List popActions() {
        List result = null;
        if (!this.actions.isEmpty()) {
            result = this.actions;
            this.actions = new LinkedList();
        } else {
            result = Collections.EMPTY_LIST;
        }
        return result;
    }

    /**
     * Remove the specified number of modification objects that are in the queue and return them.
     * @param count the number of modification action objects to remove from the queue; if greater than
     * the number of actions in this queue, all of the actions are popped.
     * @return the list of actions that was removed from the queue, ordered by the time of their creation
     * with the action created last at the end of the list, or an empty List if there are no actions in the queue.
     */
    public synchronized List popActions(int count) {
        int stop = count < this.actions.size() ? count : this.actions.size();
        if (stop == 0) {
            return Collections.EMPTY_LIST;
        }
        List result = new LinkedList(this.actions);
        return result;
    }

    /**
     * Return a clone of the last modification action object that was added to this queue.  This method does
     * not alter the queue in any way.
     * @return a clone of the last modification action instance added to this queue.
     * @throws NoSuchElementException if this queue is empty.
     */
    public ActionDefinition getLast() throws NoSuchElementException {
        ActionDefinition lastAction = null;
        synchronized( this.actions ) {
            lastAction = (ActionDefinition) this.actions.getLast();
        }
        return (ActionDefinition) lastAction.clone();
    }

    /**
     * Remove and return the last modification action object that was added to this queue.  This method
     * reduces the number of actions in the queue by 1.
     * @return a clone of the last modification action instance added to this queue.
     * @throws NoSuchElementException if this queue is empty.
     */
    public ActionDefinition removeLast() throws NoSuchElementException {
        Object result = null;
        synchronized( this.actions ) {
            result = this.actions.removeLast();
        }
        return (ActionDefinition) result;
    }

    /**
     * Add to the end of this queue a modification action.
     * @param newAction the new modification action that is to be added to this queue.
     * @boolean true if the action could be added, or false
     * otherwise.
     * @throws IllegalArgumentException if the new action is null.
     */
    public boolean addAction(ActionDefinition newAction) {
        if (newAction == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.ACTIONS_ERR_0006));

        }
        synchronized( this.actions ) {
            this.actions.add(newAction);
        }
        return true;
    }

    /**
     * Moved to the end of this queue the list of modification actions.
     * @param ordered list of actions to be added to this queue.
     * @return the number of actions added to this queue.
     * @throws IllegalArgumentException if the new action is null.
     */
    public int addActions( List newActions ) {
        if (newActions == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.ACTIONS_ERR_0007));

        }
        synchronized( this.actions ) {
            this.actions.addAll(newActions);
        }
        return newActions.size();
    }

    /**
     * Moved to the end of this queue the modification actions in the specified queue.
     * The actions will be removed from the specified queue.
     * @param queue the queue from with all actions are to be removed and added
     * to this queue.
     * @return the number of actions added to this queue.
     * @throws IllegalArgumentException if the new action is null.
     */
    public int addActions( ModificationActionQueue queue ) {
        if (queue == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.ACTIONS_ERR_0008));

        }
        List newActions = queue.popActions();       // may block
        synchronized( this.actions ) {
            this.actions.addAll(newActions);
        }
        return newActions.size();
    }

    /**
     * Return an unmodifiable copy of the list of actions in this queue,
     * possibly empty but never null
     * @return the unmodifiable list of actions in this queue, possibly
     * empty but never null
     */
    public List getActions() {
        List result = null;
        synchronized( this.actions ) {
            result = new ArrayList( this.actions );
        }
        return Collections.unmodifiableList( result );
    }

    /**
     * Return the number of actions that are currently in this queue.
     * @return the number of actions in this queue.
     */
    public synchronized int getActionCount() {
        return this.actions.size();
    }

    /**
     * Return whether this queue has at least one modification action.
     * @return true if there is at least one modification action in this queue, or false if there are none.
     */
    public synchronized boolean hasActions() {
        return !this.actions.isEmpty();
    }

    /**
     * Remove all of the actions that are currently in this queue.
     */
    public synchronized void clear() {
        this.actions.clear();
    }

    public synchronized String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("BasicModificationActionQueue contents\n"); //$NON-NLS-1$
        sb.append("  Action count      : ");    sb.append( this.actions.size() ); //$NON-NLS-1$
        sb.append('\n');
        Iterator iter = this.actions.iterator();
        ActionDefinition action = null;
        for (int i=0; iter.hasNext(); ++i ) {
            action = (ActionDefinition) iter.next();
            sb.append("  Action #"); //$NON-NLS-1$
            sb.append(i);
            sb.append(": "); //$NON-NLS-1$
            sb.append(action.toString());
            sb.append('\n');
        }
        return sb.toString();
    }
}





