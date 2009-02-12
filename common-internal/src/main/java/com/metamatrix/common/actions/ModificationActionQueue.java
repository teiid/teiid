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

import java.util.List;

/**
 * This interface defines a queue of modification actions.  Methods exist to pop the actions, add an
 * action, clear the actions, and get the number of actions.
 * <p>
 * After a modification action is created, it may be placed within this queue.  At some determined
 * point (i.e., a user presses an "apply" button), the actions in this queue may be popped out of the
 * queue and executed as an atomic transaction.
 */
public interface ModificationActionQueue {

    /**
     *@link aggregation
     * @supplierCardinality 0..*
     * @label actions
     */

    /*#ActionDefinition lnkModificationAction;*/

    /**
     * Remove all of the modification objects that are in the queue and return them.  Immediately after this
     * method is called, the queue will contain no actions.
     * @return the list of actions, ordered by the time of their creation with the action created last at the end
     * of the list; may be an empty List if there are no actions, but null
     * should not be returned
     */
    List popActions();

    /**
     * Remove the specified number of modification objects that are in the queue and return them.
     * @param count the number of modification action objects to remove from the queue; if greater than
     * the number of actions in this queue, all of the actions are popped.
     * @return the list of actions that was removed from the queue, ordered by the time of their creation
     * with the action created last at the end of the list, or an empty List if there are no actions in the queue.
     */
    List popActions(int count);

    /**
     * Return a clone of the last modification action object that was added to this queue.  This method does
     * not alter the queue in any way.
     * @return a clone of the last modification action instance added to this queue.
     */
    ActionDefinition getLast();

    /**
     * Remove and return the last modification action object that was added to this queue.  This method
     * reduces the number of actions in the queue by 1.
     * @return a clone of the last modification action instance added to this queue.
     */
    ActionDefinition removeLast();

    /**
     * Add to the end of this queue a modification action.
     * @param newAction the new modification action that is to be added to this queue.
     * @return true if the action could be added, or false
     * otherwise.
     * @throws IllegalArgumentException if the new action is null.
     */
    boolean addAction(ActionDefinition newAction);

    /**
     * Moved to the end of this queue the list of modification actions.
     * @param ordered list of actions to be added to this queue.
     * @return the number of actions added to this queue.
     * @throws IllegalArgumentException if the new action is null.
     */
    int addActions( List newActions );

    /**
     * Moved to the end of this queue the modification actions in the specified queue.
     * The actions will be removed from the specified queue.
     * @param queue the queue from with all actions are to be removed and added
     * to this queue.
     * @return the number of actions added to this queue.
     * @throws IllegalArgumentException if the new action is null.
     */
    int addActions( ModificationActionQueue queue );

    /**
     * Return a copy of the list of actions in this queue,
     * possibly empty but never null
     * @return the list of actions in this queue, possibly
     * empty but never null
     */
    List getActions();

    /**
     * Return the number of actions that are currently in this queue.
     * @return the number of actions in this queue.
     */
    int getActionCount();

    /**
     * Return whether this queue has at least one modification action.
     * @return true if there is at least one modification action in this queue, or false if there are none.
     */
    boolean hasActions();

    /**
     * Remove all of the actions that are currently in this queue.
     */
    void clear();
}



