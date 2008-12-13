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

import com.metamatrix.common.id.TransactionID;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.TransactionStatus;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;

public class ConfigUserTransaction implements UserTransaction {

    private static final int TIMEOUT_NOT_SPECIFIED = -1;

    /** The underlying "real" transaction to which this user transaction is bound */
    private ConfigTransaction txn;

    /** Used to track the status when this user transaction is not bound to an underlying transaction */
    private int status;

    /**
     * The object that is considered the source of this transaction and the
     * underlying transaction
     */
    private Object source;

    private boolean readTxn;

    private int timeout;
    private boolean alreadyBegun;
    private ConfigTransactionManager mgr;
//	private ConfigTransactionLockFactory configLockFactory;
    private String name;

    ConfigUserTransaction( Object source, boolean isReadOnly, ConfigTransactionManager txnManager, String name )  {
        Assertion.isNotNull(txnManager,ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0045, "ConfigTransactionManager")); //$NON-NLS-1$
        this.source = source;
        this.readTxn = isReadOnly;
        this.status = TransactionStatus.STATUS_UNKNOWN;
        this.timeout = TIMEOUT_NOT_SPECIFIED;
        this.mgr = txnManager;
        this.name = name;
//        this.configLockFactory = null;


    }

    String getName() {
        return this.name;
    }

    public Object getSource() {
        return this.source;
    }

    public TransactionID getTransactionID() {
        return this.mgr.getTransactionID();
    }

    public void begin() throws ConfigTransactionLockException, TransactionException {
        if ( alreadyBegun ) {
            throw new TransactionException(ConfigMessages.CONFIG_0072, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0072));
        }
        if ( this.status != TransactionStatus.STATUS_UNKNOWN ) {
            throw new TransactionException(ConfigMessages.CONFIG_0073, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0073));
        }
//System.out.println("<CFG USER TRANS>MGR GET TRANS");
    	// trigger the manager that this thread is starting a transaction
    	this.mgr.begin();

        this.txn = (ConfigTransaction) this.mgr.getTransaction();
        this.txn.begin(name, ConfigTransactionLock.LOCK_CONFIG_CHANGING, this.readTxn);
        this.txn.setSource(this.source);

        if (!this.readTxn) {
        	 if (this.txn.getTransactionLock() == null) {
            	throw new ConfigTransactionLockException(ConfigMessages.CONFIG_0074, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0074));

        	 }

        }
        if ( this.timeout != TIMEOUT_NOT_SPECIFIED ) {
            this.txn.setTransactionTimeout(this.timeout);
        }
// this needs to be last so that it officially has been begun until this step has completed
// also, its so commit cannot be called until the begin has completed.
		this.alreadyBegun = true;

//System.out.println("<CFG USER TRANS>ENDED BEGIN TRANS");

    }
    public void commit() throws ConfigTransactionLockException, TransactionException {
        if ( ! alreadyBegun ) {
            throw new TransactionException(ConfigMessages.CONFIG_0075, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075));
        }
        if ( this.txn == null ) {
            throw new TransactionException(ConfigMessages.CONFIG_0076, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0076));
        }

        if (this.txn.hasExpired()) {
        	String lockFor = this.txn.getTransactionLock().toString();
            throw new TransactionException(ConfigMessages.CONFIG_0077, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0077, lockFor));

        }
//System.out.println("<CFG USER TRANS>COMMIT TRANS");

        this.mgr.commit();

                // Get status so we always have it
        this.status = this.txn.getStatus();
        this.txn = null;        // we're done with the underlying transaction
//System.out.println("<CFG USER TRANS>RETURN FROM COMMIT TRANS");

    }
    public int getStatus() throws TransactionException {
        // Return this object's status only if there is no underlying transaction
        if ( this.txn == null ) {
            return this.status;
        }

        // Otherwise, return the status of the transaction to which this object is still bound
        return this.txn.getStatus();
    }
    public void rollback() throws TransactionException {
        if (this.mgr.hasTransaction()) {
            // Want to make sure the release of the lock is performed
            // regardless if the rollback is successfull because an error in the
            // begin() method could trigger a rollback, otherwise
            // a leftover reference is held in the mgr and an exception
            // is thrown that only 1 thread can be in the transaction
            this.mgr.rollback();
        }

        if ( ! alreadyBegun ) {
            return;     // there's nothing to do, 'cause this was likely caused after 'begin' failed
        }
        if ( this.txn == null ) {
            throw new TransactionException(ConfigMessages.CONFIG_0076, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0076));
        }

        // Get status so we always have it
        this.status = this.txn.getStatus();
        this.txn = null;        // we're done with the underlying transaction
    }
    public void setRollbackOnly() throws TransactionException {
        if ( ! alreadyBegun ) {
            return;     // there's nothing to do, 'cause this was likely caused after 'begin' failed
        }
        if ( this.txn == null ) {
            throw new TransactionException(ConfigMessages.CONFIG_0076, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0076));
        }
        this.txn.setRollbackOnly();
    }
    public void setTransactionTimeout(int seconds) throws TransactionException {
        if ( alreadyBegun && this.txn == null ) {
            throw new TransactionException(ConfigMessages.CONFIG_0076, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0076));
        }
        if ( seconds <= 0 ) {
			throw new IllegalArgumentException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0078));
        }
        if ( this.txn != null ) {
            this.txn.setTransactionTimeout(seconds);        // could throw exception ???
        }
        this.timeout = seconds;     // set this after setting timeout on real txn succeeds
    }

    public boolean isReadTransaction() {
    	return readTxn;
    }


    public ConfigTransaction getTransaction() {
    	return txn;
    }

}

