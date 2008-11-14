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

package com.metamatrix.common.transaction.manager;

import com.metamatrix.common.transaction.TransactionException;

public interface TransactionManager {

    /**
     * Obtain the status of the transaction associated with the current thread.
     * @return The transaction status. If no transaction is associated with
     * the current thread, this method returns the TransactionStatus.NoTransaction value
     */
    int getStatus() throws TransactionException;

    /**
     * Create a new transaction and associate it with the current thread.
     * @throws TransactionNotSupportedException if the current thread is already
     * associated with a transaction and the manager does not support
     * nested system transactions.
     */
    void begin() throws TransactionException;

    /**
     * Get the transaction object that represents the transaction context
     * of the calling thread.
     * @return the Transaction object representing the transaction associated
     * with the calling thread.
     */
    Transaction getTransaction() throws TransactionException;

    /**
     * Suspend the transaction currently associated with the calling thread
     * and return a Transaction object that represents the transaction context
     * being suspended. If the calling thread is not associated with a
     * transaction, the method returns a null object reference. When
     * this method returns, the calling thread is associated with no transaction.
     * @returns The Transaction object representing the suspended transaction.
     */
    Transaction suspend() throws TransactionException;

    /**
     * Resume the transaction context association of the calling thread
     * with the transaction represented by the supplied Transaction object.
     * When this method returns, the calling thread is associated with the
     * transaction context specified.
     * @param txnObj The Transaction object that represents the transaction to be resumed.
     * @throws IllegalStateException Thrown if the thread is already associated with another transaction
     */
    void resume(Transaction txnObj) throws TransactionException;

    /**
     * Modify the value of the timeout value that is associated with the
     * transactions started by the current thread with the begin method.
     * If an application has not called this method, the transaction service
     * uses some default value for the transaction timeout.
     * @param seconds The value of the timeout in seconds. If the value is
     * zero, the transaction service restores the default value.
     * @throws IllegalStateException Thrown if the current thread is not associated with a transaction
     */
    void setTransactionTimeout(int seconds) throws TransactionException;

    /**
     * Modify the transaction associated with the current thread such that
     * the only possible outcome of the transaction is to roll back the transaction.
     * @throws IllegalStateException Thrown if the current thread is not
     * associated with a transaction.
     */
    void setRollbackOnly() throws TransactionException;

    /**
     * Complete the transaction associated with the current thread.
     * When this method completes, the thread becomes associated with no transaction.
     * @throws IllegalStateException Thrown if the current thread is not
     * associated with a transaction.
     */
    void commit() throws TransactionException;

    /**
     * Roll back the transaction associated with the current thread.
     * When this method completes, the thread becomes associated with no
     * transaction.
     * @throws IllegalStateException Thrown if the current thread is not
     * associated with a transaction.
     */
    void rollback() throws TransactionException;
}

