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

public interface UserTransaction {

    /**
     * Obtain the status of the transaction represented by this object.
     * @return The transaction status.
     */
    int getStatus() throws TransactionException;

    /**
     * Create a new transaction and associate it with this object.
     * @throws TransactionNotSupportedException if the current thread is already
     * associated with a transaction and the manager does not support
     * nested system transactions.
     */
    void begin() throws TransactionException;

    /**
     * Modify the value of the timeout value that is associated with the
     * transactions represented by this object.
     * If an application has not called this method, the transaction service
     * uses some default value for the transaction timeout.
     * @param seconds The value of the timeout in seconds. If the value is
     * zero, the transaction service restores the default value.
     * @throws IllegalStateException Thrown if this object is not associated with a transaction
     */
    void setTransactionTimeout(int seconds) throws TransactionException;

    /**
     * Modify the transaction associated with this object such that
     * the only possible outcome of the transaction is to roll back the transaction.
     * @throws IllegalStateException Thrown if this object is not
     * associated with a transaction.
     */
    void setRollbackOnly() throws TransactionException;

    /**
     * Complete the transaction associated with this object.
     * When this method completes, the thread becomes associated with no transaction.
     * @throws IllegalStateException Thrown if this object is not
     * associated with a transaction.
     */
    void commit() throws TransactionException;

    /**
     * Roll back the transaction associated with this object.
     * When this method completes, the thread becomes associated with no
     * transaction.
     * @throws IllegalStateException Thrown if this object is not
     * associated with a transaction.
     */
    void rollback() throws TransactionException;

    /**
     * Return the (optional) reference to the object that is considered
     * the source of the transaction represented by this object.
     * This is used, for example, to set the source of all events occuring within this
     * transaction.
     * @return the source object, which may be null
     */
    Object getSource() throws TransactionException;

}

