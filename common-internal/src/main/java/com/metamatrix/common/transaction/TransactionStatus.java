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

package com.metamatrix.common.transaction;

public interface TransactionStatus {

    /**
     * A transaction is associated with the target object but its current
     * status cannot be determined. This is a transient condition and a subsequent
     * invocation will ultimately return a different status.
     */
    int STATUS_UNKNOWN = 5;

    /**
     * A transaction is associated with the target object and it is in the
     * active state. An implementation returns this status after a
     * transaction has been started and prior to a Coordinator issuing
     * any prepares unless the transaction has been marked for rollback.
     */
    int STATUS_ACTIVE = 0;

    /**
     * A transaction is associated with the target object and it is in
     * the process of committing. An implementation returns this status
     * if it has decided to commit, but has not yet completed the process,
     * probably because it is waiting for responses from one or more Resources.
     */
    int STATUS_COMMITTING = 8;

    /**
     * A transaction is associated with the target object and it has been
     * committed. It is likely that heuristics exists, otherwise the
     * transaction would have been destroyed and NoTransaction returned.
     */
    int STATUS_COMMITTED = 3;

    /**
     * A transaction is associated with the target object and it has been
     * marked for rollback, perhaps as a result of a setRollbackOnly operation
     */
    int STATUS_MARKED_ROLLBACK = 1;

    /**
     * A transaction is associated with the target object and it is in
     * the process of preparing. An implementation returns this status
     * if it has started preparing, but has not yet completed the process,
     * probably because it is waiting for responses to prepare from one or
     * more Resources
     */
    int STATUS_PREPARING = 7;

    /**
     * A transaction is associated with the target object and it has
     * been prepared, i.e. all subordinates have responded Vote.Commit.
     * The target object may be waiting for a superior's instruction as
     * how to proceed.
     */
    int STATUS_PREPARED = 2;

    /**
     * A transaction is associated with the target object and it is
     * in the process of rolling back. An implementation returns this
     * status if it has decided to rollback, but has not yet completed the
     * process, probably because it is waiting for responses from one or more Resources.
     */
    int STATUS_ROLLING_BACK = 9;

    /**
     * A transaction is associated with the target object and the outcome
     * has been determined as rollback. It is likely that heuristics exist,
     * otherwise the transaction would have been destroyed and NoTransaction
     * returned.
     */
    int STATUS_ROLLEDBACK = 4;

    /**
     * No transaction is currently associated with the target object.
     * This will occur after a transaction has completed.
     */
    int STATUS_NO_TRANSACTION = 6;

}
