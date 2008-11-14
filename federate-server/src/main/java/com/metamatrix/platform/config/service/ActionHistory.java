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

package com.metamatrix.platform.config.service;

import java.util.*;

import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.util.ErrorMessageKeys;

/**
 * This class maintains a history of actions.  The "maximum size" of
 * the history can be specified.  However, when the history is pruned,
 * it is done so in whole transactions.
 *
 */
public class ActionHistory {

    public static final int DEFAULT_ACTION_COUNT_LIMIT = 100;

    private LinkedList actions = new LinkedList();
    private LinkedList actionCountInTransactions = new LinkedList();
    private int actionCountLimit = DEFAULT_ACTION_COUNT_LIMIT;

    public ActionHistory() {
    }

    public synchronized void addActionsForTransaction( List newActions ) {
        if ( newActions == null || newActions.isEmpty() ) {
            return;
        }

        // Add the actions to the history ...
        Iterator iter = newActions.iterator();
        while ( iter.hasNext() ) {
            this.actions.addFirst( iter.next() );
        }
        this.actionCountInTransactions.addFirst( new Integer(newActions.size()) );

        // Prune the history if required ...
        this.pruneIfRequired();
    }

    public synchronized int getHistorySize() {
        return this.actions.size();
    }

    public synchronized List getHistory() {
        return this.actions;
    }

    public synchronized List pop( int count ) {
        List result = new LinkedList();
        if ( count > 0 ) {
            int actualCount = count;
            if ( actualCount > this.actions.size() ) {
                actualCount = this.actions.size();
            }
            for ( int i=0; i!=actualCount; ++i ) {
                result.add( this.actions.removeFirst() );
            }
        }
        return result;
    }

    public int getHistoryLimit() {
        return this.actionCountLimit;
    }

    public synchronized void clearHistory() {
        this.actions.clear();
        this.actionCountInTransactions.clear();
    }

    public synchronized void setHistoryLimit( int newLimit ) {
        if ( newLimit < 0 ) {
            throw new IllegalArgumentException(PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0037));
        }
        this.actionCountLimit = newLimit;
        this.pruneIfRequired();
    }

    protected void pruneIfRequired() {
        int newSize = this.actions.size();

        // If the new size is greater than the limit, check whether the history
        // should be shrunk ...
        if ( newSize > actionCountLimit ) {
            // Get the number of actions in the earliest transaction.
            Integer countInEarliestTransaction = (Integer) actionCountInTransactions.getLast();

            // Remove all of the actions in the transaction ONLY IF the history
            // would we still be greater than the limit ...
            int numberToBeRemoved = countInEarliestTransaction.intValue();
            if ( newSize - numberToBeRemoved > actionCountLimit ) {
                for ( int i=0; i!= numberToBeRemoved; ++i ) {
                    this.actions.removeLast();
                }
                actionCountInTransactions.removeLast();
            }
        }
    }

}

