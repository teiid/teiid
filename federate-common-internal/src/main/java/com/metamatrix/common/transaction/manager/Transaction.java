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

public interface Transaction {

    /**
     * Obtain the status of the transaction associated with this object.
     * @return The transaction status.
     * @throws TransactionException if the status for this transaction could
     * not be obtained.
     */
    int getStatus() throws TransactionException;

    /**
     * Modify the value of the timeout value that is associated with this object.
     * @param seconds The value of the timeout in seconds. If the value is
     * zero, the transaction service restores the default value.
     * @throws TransactionException if the timeout value is unable to be set
     * for this transaction.
     */
    void setTransactionTimeout(int seconds) throws TransactionException;

    /**
     * Modify the transaction such that the only possible outcome of the transaction
     * is to roll back the transaction.
     * @throws TransactionException if the rollback flag is unable to be set
     * for this transaction.
     */
    void setRollbackOnly() throws TransactionException;

    /**
     * Complete the transaction represented by this TransactionObject.
     * @throws TransactionException if the transaction is unable to commit.
     */
    void commit() throws TransactionException;

    /**
     * Roll back the transaction represented by this TransactionObject.
     * @throws TransactionException if the transaction is unable to roll back.
     */
    void rollback() throws TransactionException;
}

