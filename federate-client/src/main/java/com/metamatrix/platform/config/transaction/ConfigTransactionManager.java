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

package com.metamatrix.platform.config.transaction;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.common.id.TransactionID;
import com.metamatrix.common.id.TransactionIDFactory;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.TransactionNotSupportedException;
import com.metamatrix.common.transaction.TransactionStatus;
import com.metamatrix.common.transaction.manager.Transaction;
import com.metamatrix.common.transaction.manager.TransactionManager;
import com.metamatrix.core.id.ObjectIDFactory;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;


public class ConfigTransactionManager implements TransactionManager {
    private int defaultTimeout;
    private Map txnsByThread;
    private ObjectIDFactory idFactory;
	private ConfigTransactionFactory transFactory;

    private static final int DEFAULT_TIMEOUT = 300000; // 5 mins

    public ConfigTransactionManager( ConfigTransactionFactory transFactory, int defaultTimeout ) {
        if ( defaultTimeout <= 0 ) {
            throw new IllegalArgumentException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0165));
        }
        this.defaultTimeout = defaultTimeout;
        this.txnsByThread = new HashMap();
        this.idFactory = new TransactionIDFactory();
        this.transFactory = transFactory;
    }

    public ConfigTransactionManager( ConfigTransactionFactory transFactory ) {
    	this(transFactory, DEFAULT_TIMEOUT);
    }


    /**
     * Obtain the status of the transaction associated with the current thread.
     * @return The transaction status. If no transaction is associated with
     * the current thread, this method returns the TransactionStatus.NoTransaction value
     */
    public synchronized int getStatus() throws TransactionException {
        Transaction txn = (Transaction) this.txnsByThread.get( Thread.currentThread() );
        int status = TransactionStatus.STATUS_NO_TRANSACTION;
        if ( txn != null ) {
            status = txn.getStatus();
        }
        return status;
    }

    /**
     * Create a new transaction and associate it with the current thread.
     * @throws TransactionNotSupportedException if the current thread is already
     * associated with a transaction and the manager does not support
     * nested system transactions.
     */
    public synchronized void begin() throws TransactionException {
        Thread currentThread = Thread.currentThread();
        ConfigTransaction txn = (ConfigTransaction) this.txnsByThread.get( currentThread );
        if ( txn != null ) {
            throw new TransactionNotSupportedException(ConfigMessages.CONFIG_0166, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0166));
        }
        TransactionID id = (TransactionID) this.idFactory.create();

        txn = transFactory.createConfigTransaction(id, this.defaultTimeout);
        this.txnsByThread.put( currentThread, txn );


    }

    /**
     * Get the transaction object that represents the transaction context
     * of the calling thread.  If a transaction is not associated with the calling
     * thread, one is created and begun.
     * @return the Transaction object representing the transaction associated
     * with the calling thread.
     */
    public synchronized Transaction getTransaction() throws TransactionException {
        Thread currentThread = Thread.currentThread();
        Transaction txn = (Transaction) this.txnsByThread.get( currentThread );
        if ( txn == null ) {
        	 throw new TransactionException(ConfigMessages.CONFIG_0167, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0167));

// **************************************************************************************************
// TODO: Throwing the exception will force all users to have a transaction; this
//       isn't what the Modeler team wants yet.
//            throw new TransactionException("There is no transaction associated with the current thread");
// **************************************************************************************************
//            TransactionID id = (TransactionID) this.idFactory.create();
//            txn = new ToolkitTransaction(id, this.defaultTimeout, new UndoableToolkitEdit() );
//            this.txnsByThread.put( Thread.currentThread(), txn );
//            ToolkitLog.logDetail(ToolkitLogConstants.CTX_TXN,new Object[]{"Associating transaction ",id," with thread ",currentThread.getName()});
        }
        return txn;
    }



    /**
     * Get the transaction identifier that represents the transaction context
     * of the calling thread.  If a transaction is not associated with the calling
     * thread, one is created and begun.
     * @return the TransactionID object representing the transaction associated
     * with the calling thread; may be null
     */
    synchronized TransactionID getTransactionID() {
        TransactionID txnID = null;
        Thread currentThread = Thread.currentThread();
        ConfigTransaction txn = (ConfigTransaction) this.txnsByThread.get( currentThread );
        if ( txn != null ) {
            txnID = txn.getTransactionID();
        }
        return txnID;
    }

    /**
     * Determine whether there is a transaction associated with the calling thread.
     * @return true if there is a transaction, or false if there is no transaction.
     */
    public synchronized boolean hasTransaction() throws TransactionException {
        return this.txnsByThread.get( Thread.currentThread() ) != null;
    }

    /**
     * Suspend the transaction currently associated with the calling thread
     * and return a Transaction object that represents the transaction context
     * being suspended. If the calling thread is not associated with a
     * transaction, the method returns a null object reference. When
     * this method returns, the calling thread is associated with no transaction.
     * @returns The Transaction object representing the suspended transaction.
     */
    public synchronized Transaction suspend() throws TransactionException {
        Thread currentThread = Thread.currentThread();
        ConfigTransaction txn = (ConfigTransaction) this.txnsByThread.remove( currentThread );
        if ( txn != null ) {
            // TODO: Do something here?
        }
        return txn;
    }

    /**
     * Resume the transaction context association of the calling thread
     * with the transaction represented by the supplied Transaction object.
     * When this method returns, the calling thread is associated with the
     * transaction context specified.
     * @param txnObj The Transaction object that represents the transaction to be resumed.
     * @throws IllegalStateException Thrown if the thread is already associated with another transaction
     */
    public synchronized void resume(Transaction txnObj) throws TransactionException {
        ConfigTransaction txn = (ConfigTransaction) this.txnsByThread.get( Thread.currentThread() );
        if ( txn != null ) {
            throw new IllegalStateException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0168));
        }
        // TODO: Do something to the txnObj here?
        if ( txnObj instanceof ConfigTransaction ) {
            txn = (ConfigTransaction) txnObj;
            this.txnsByThread.put( Thread.currentThread(), txn );
        } else {
            throw new IllegalArgumentException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0169));
        }
    }

    /**
     * Modify the value of the timeout value that is associated with the
     * transactions started by the current thread with the begin method.
     * If an application has not called this method, the transaction service
     * uses some default value for the transaction timeout.
     * @param seconds The value of the timeout in seconds. If the value is
     * zero, the transaction service restores the default value.
     * @throws IllegalStateException Thrown if the current thread is not associated with a transaction
     */
    public synchronized void setTransactionTimeout(int seconds) throws TransactionException {
        Transaction txn = (Transaction) this.txnsByThread.get( Thread.currentThread() );
        if ( txn == null ) {
            throw new IllegalStateException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0170));
        }
        txn.setTransactionTimeout(seconds);
    }

    /**
     * Modify the transaction associated with the current thread such that
     * the only possible outcome of the transaction is to roll back the transaction.
     * @throws IllegalStateException Thrown if the current thread is not
     * associated with a transaction.
     */
    public synchronized void setRollbackOnly() throws TransactionException {
        ConfigTransaction txn = (ConfigTransaction) this.txnsByThread.get( Thread.currentThread() );
        if ( txn == null ) {
            throw new IllegalStateException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0170));
        }
        txn.setRollbackOnly();
    }

    /**
     * Complete the transaction associated with the current thread.
     * When this method completes, the thread becomes associated with no transaction.
     * @throws IllegalStateException Thrown if the current thread is not
     * associated with a transaction.
     */
    public synchronized void commit() throws TransactionException {
        Thread currentThread = Thread.currentThread();
        ConfigTransaction txn = (ConfigTransaction) this.txnsByThread.remove( currentThread );
        if ( txn == null ) {
            throw new IllegalStateException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0170));
        }

        txn.commit();

//        System.out.println("ConfigTransMgr committed ConfigTransaction");
    }

    /**
     * Roll back the transaction associated with the current thread.
     * When this method completes, the thread becomes associated with no
     * transaction.
     * @throws IllegalStateException Thrown if the current thread is not
     * associated with a transaction.
     */
    public synchronized void rollback() throws TransactionException {
        Thread currentThread = Thread.currentThread();
        ConfigTransaction txn = (ConfigTransaction) this.txnsByThread.remove( currentThread );
        if ( txn == null ) {
            return;
            //throw new IllegalStateException("The current thread is not associated with a transaction.");
            //
            // Return rather than throw exception because ...
            //
            // There is difficulty when coding transactions and try/catch, when
            // this method throws an exception.  For example,
            //     UserTransaction txn = session.createReadTransaction();
            //    boolean success = false;
            //    try {
            //        txn.begin();        // WHAT IF THIS THROWS TransactionException ???
            //        // do stuff here
            //        success = true;
            //    } catch ( Exception e ) {
            //        success = false;
            //    } finally {
            //        if ( success ) {
            //            try {
            //                txn.commit();
            //            } catch ( TransactionException e ) {
            //            }
            //        } else {
            //            try {
            //                txn.rollback();     // THEN THIS WILL FAIL !!!!!
            //            } catch ( TransactionException e ) {
            //            }
            //        }
            //    }
            //
            // By just returning rather than throwing an exception, then the
            // call to rollback won't fail if a transaction was never begun.
            //
        }
        txn.rollback();
    }
}

