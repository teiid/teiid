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

package com.metamatrix.common.connection;

/**
 * This new interface should actually be in com.metamatrix.common.connection. 
 */
public interface TransactionInterface {
    /**
     * Make all changes made during this transaction's lifetime
     * and release any data source locks currently held by the associated Connection.
     * A transaction can be committed or rolled back any number of times throughout its lifetime,
     * and throughout its lifetime the transaction is guaranteed to have the same connection.
     * @throws ManagedConnectionException if an error occurred within or during communication with the associated connection.
     */
    void commit() throws ManagedConnectionException;

    /**
     * Drops all changes made during this transaction's lifetime
     * and release any data source locks currently held by the associated Connection.
     * Once this method is executed, the transaction (after rolling back) becomes invalid, and the connection
     * referenced by this transaction is returned to the pool.
     * <p>
     * Calling this method on a read-only transaction is unneccessary (and discouraged, since
     * the implementation does nothing in that case anyway).
     * @throws ManagedConnectionException if an error occurred within or during communication with the associated connection.
     */
    void rollback() throws ManagedConnectionException;
    /**
     * Return whether this transaction is readonly.
     * @return true if this transaction is readonly, or false otherwise.
     */
    boolean isReadonly();
    /**
     * Return whether this transaction is readonly.
     * @return true if this transaction is readonly, or false otherwise.
     */
    boolean isClosed();
    /**
     * Return whether this transaction is readonly.
     * @return true if this transaction is readonly, or false otherwise.
     */
    void close();
}

