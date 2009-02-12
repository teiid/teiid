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

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

public abstract class BaseTransaction implements TransactionInterface {

    private static boolean ROLLBACK_ON_FINALIZE = true;

    private ManagedConnection connection;
    private boolean readonly = true;

    // Flag indicating that the transaction has ended with a commit or rollback
    private boolean isEnded = false;

    protected void finalize() {
        this.close();
    }

    protected BaseTransaction(ManagedConnection connection, boolean readonly )
    throws ManagedConnectionException {
        if ( connection == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0001));
        }
        this.readonly = readonly;
        this.connection = connection;
    }

    /**
     * Make all changes made during this transaction's lifetime
     * and release any data source locks currently held by the associated Connection.
     * A transaction can be committed or rolled back any number of times throughout its lifetime,
     * and throughout its lifetime the transaction is guaranteed to have the same connection.
     * @throws ManagedConnectionException if an error occurred within or during communication with the associated connection.
     */
    public void commit() throws ManagedConnectionException {
        if ( this.isEnded() ) {
            if ( this.isClosed() ) {
                throw new ManagedConnectionException(ErrorMessageKeys.CONNECTION_ERR_0002, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0002));
            }
            throw new ManagedConnectionException(ErrorMessageKeys.CONNECTION_ERR_0003, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0003));
        }

        this.connection.commit();
        this.isEnded = true;
    }

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
    public void rollback() throws ManagedConnectionException {
        if ( this.isEnded() ) {
            if ( this.isClosed() ) {
                throw new ManagedConnectionException(ErrorMessageKeys.CONNECTION_ERR_0002, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0002));
            }
            throw new ManagedConnectionException(ErrorMessageKeys.CONNECTION_ERR_0003, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0003));
        }

        // Invoking rollback on a read-only transaction will interrupt other
        // readers.  Therefore, only rollback in write transactions, where this
        // transaction is the only user of the connection.
        if ( ! this.isReadonly() ) {
            this.connection.rollback();
            this.isEnded = true;
        }
    }

    /**
     * Return the connection associated with this transaction.  NOTE: the connection
     * returned by this method is returned automatically when the transaction
     * is committed or rolled back.  Therefore, the connection reference should
     * <i>not</i> be held by the caller; doing so and invoking a method upon
     * the connection after the transaction has completed is undefined.
     * @return the managed connection for this transaction.
     */
    public ManagedConnection getConnection() throws ManagedConnectionException {
        if ( this.isClosed() ) {
                throw new ManagedConnectionException(ErrorMessageKeys.CONNECTION_ERR_0002, CommonPlugin.Util.getString(ErrorMessageKeys.CONNECTION_ERR_0002));
        }
        return this.connection;
    }

    /**
     * Return whether this transaction is readonly.
     * @return true if this transaction is readonly, or false otherwise.
     */
    public final boolean isReadonly() {
        return this.readonly;
    }

    /**
     * Return whether this transaction is ended.
     * @return true if this transaction has ended with a commit or rollback.
     */
    public final boolean isEnded() {
        return this.isEnded;
    }

    /**
     * Return whether this transaction is readonly.
     * @return true if this transaction is readonly, or false otherwise.
     */
    public final boolean isClosed() {
        return this.connection == null;
    }

    /**
     * Return whether this transaction is readonly.
     * @return true if this transaction is readonly, or false otherwise.
     */
    public void close() {
        if ( ! this.isClosed() ) {
            try {
                if ( this.isReadonly() ) {
//                    LogManager.logInfo("MT","**** Closing Read Tranaction ***");

                    if ( ! this.isEnded()) {
                        this.commit();
                    }
                    this.connection.close();
                } else {
                    // Only end the transaction if it needs it
                    if( ! this.isEnded()) {
                        if ( ROLLBACK_ON_FINALIZE ) {
                            this.rollback();
                        } else {
                            this.commit();
                        }
                    }
//               LogManager.logInfo("MT","**** Closing Write Tranaction ***");

                    this.connection.close();
                }
            } catch ( ManagedConnectionException e ) {
            } finally {
                this.connection = null;
            }
        } else {
//               LogManager.logInfo("MT","**** Tranaction NOT CLOSING ***");

        }
    }

    /**
     * Return whether transactions, if they have not been committed or rolled back
     * when they are destroyed, are rolled back or committed.
     * @return true if transactions are rolled back when destroyed if not previously
     * committed or rolled back.
     */
    public static final boolean getRollbackOnFinalize() {
        return ROLLBACK_ON_FINALIZE;
    }

    /**
     * Set whether transactions, if they have not been committed or rolled back
     * when they are destroyed, are rolled back or committed.
     * @param rollback true if transactions should be rolled back when destroyed if not previously
     * committed or rolled back.
     */
    public static final void setRollbackOnFinalize( boolean rollback ) {
        ROLLBACK_ON_FINALIZE = rollback;
    }


}
