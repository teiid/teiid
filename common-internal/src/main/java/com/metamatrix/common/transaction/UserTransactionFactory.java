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

package com.metamatrix.common.transaction;

public interface UserTransactionFactory {

    /**
     * Create a new instance of a UserTransaction that may be used to
     * read information.  Read transactions do not have a source object
     * associated with them (since they never directly modify data).
     * <p>
     * The returned transaction object will not be bound to an underlying
     * system transaction until <code>begin()</code> is called on the returned
     * object.
     * @return the new transaction object
     */
    UserTransaction createReadTransaction();

    /**
     * Create a new instance of a UserTransaction that may be used to
     * write and/or update information.  The transaction will <i>not</i> have a source object
     * associated with it.
     * <p>
     * The returned transaction object will not be bound to an underlying
     * system transaction until <code>begin()</code> is called on the returned
     * object.
     * @return the new transaction object
     */
    UserTransaction createWriteTransaction();

    /**
     * Create a new instance of a UserTransaction that may be used to
     * write and/or update information. The source object will be used for all events that are
     * fired as a result of or as a product of this transaction.
     * <p>
     * The returned transaction object will not be bound to an underlying
     * system transaction until <code>begin()</code> is called on the returned
     * object.
     * @param source the object that is considered to be the source of the transaction;
     * may be null
     * @return the new transaction object
     */
    UserTransaction createWriteTransaction(Object source);
}

