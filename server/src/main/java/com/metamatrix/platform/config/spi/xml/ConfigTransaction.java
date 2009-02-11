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

package com.metamatrix.platform.config.spi.xml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.common.id.TransactionID;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.TransactionStatus;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;

public class ConfigTransaction {



// codes 0 - 2 for Server states
	public static final int SERVER_INITIALIZATION = 1;
	public static final int SERVER_SHUTDOWN = 2;
	public static final int SERVER_FORCE_INITIALIZATION = 3;
	public static final int SERVER_STARTED = 4;

	public static final int NO_SERVER_INITIALIZATION_ACTION = -1;

// codes 10 and above for other states

//	private static final int DEFAULT_NO_DEFINED_ACTION = -1;

//	private static final String NON_TRANSACTION_ACQUIRED_BY = "ReadTransaction";

    private TransactionID txnID;
    private int status;
    private long beginTime;
//    private long timeout;        // 2 seconds
    private boolean isReadOnly;
    private String principal;

    private Map configurationObjects = new HashMap(3);

    // @see StartupStateController for states
    private int action = NO_SERVER_INITIALIZATION_ACTION;

    protected ConfigTransaction(TransactionID txnID, long defaultTimeoutSeconds) {
    	this.txnID = txnID;
        this.status = TransactionStatus.STATUS_ACTIVE;     //???
//        this.timeout = defaultTimeoutSeconds;
		this.isReadOnly = true;
		this.beginTime = System.currentTimeMillis();
    }

    /**
     * Obtain the status of the transaction associated with this object.
     * @return The transaction status.
     * @throws TransactionException if the status for this transaction could
     * not be obtained.
     */
    public int getStatus() {
        return this.status;
    }

    public long getBeginTime() {
    	return beginTime;
    }

    /**
     * This method is implemented by this class so that the
     * actual lock can be obtained prior to the transaction beginning.
     */
    public void begin(String principal, int reason, boolean readOnly) throws TransactionException{
		setReadOnly(readOnly);
		this.principal = principal;
    }

    /**
     * Returns the name that holds the lock.
     * @return String name who holds the lock
     */
    public String getLockAcquiredBy() {
    	return principal;
    }


    /**
     * Returns the transaction id that uniquely identifies this transaction
     * @return TransactionID that identifies the transaction
     */
    public TransactionID getTransactionID() {
        return this.txnID;
    }


    public boolean isReadOnly() {
        return this.isReadOnly;
   }

    public int getAction() {
    		return this.action;
    }

    public void setAction(int actionPerformed) {
    	// only allow the setting of the action once for the duration of the transaction
    	if (action == NO_SERVER_INITIALIZATION_ACTION) {
    		this.action = actionPerformed;
    	}
    }

    /**
     * Call to set the transaction as read only.
     * A value of <code>true</code> will indicate the transaction
     * is a read only transaction.
     * @param readTxn value of true sets the transaction to read only
     */
    void setReadOnly( boolean readTxn ) {
        this.isReadOnly = readTxn;
    }

    /**
     * Modify the transaction such that the only possible outcome of the transaction
     * is to roll back the transaction.
     * @throws TransactionException if the rollback flag is unable to be set
     * for this transaction.
     */
    public void setRollbackOnly() throws TransactionException{
        this.status = TransactionStatus.STATUS_MARKED_ROLLBACK;
    }



    /**
     * Complete the transaction represented by this TransactionObject.
     * @throws TransactionException if the transaction is unable to commit.
     */
    public void commit() throws TransactionException{
        if ( this.status == TransactionStatus.STATUS_MARKED_ROLLBACK ) {
            throw new TransactionException(ConfigMessages.CONFIG_0160, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0160));
        }
        if ( this.status != TransactionStatus.STATUS_ACTIVE ) {
            throw new TransactionException(ConfigMessages.CONFIG_0161, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0161));
        }
        this.status = TransactionStatus.STATUS_COMMITTING;

        if ( isReadOnly() ) {
            this.status = TransactionStatus.STATUS_COMMITTED;
            return;
        }

        this.status = TransactionStatus.STATUS_COMMITTED;
    }

    /**
     * Roll back the transaction represented by this TransactionObject.
     * @throws TransactionException if the transaction is unable to roll back.
     */
    public void rollback() throws TransactionException{
         if ( isReadOnly() ) {
            return;
        }

        this.status = TransactionStatus.STATUS_ROLLEDBACK;
/*
        ToolkitLog.logCritical(ToolkitLogConstants.CTX_TXN,"*******************************************");
        ToolkitLog.logCritical(ToolkitLogConstants.CTX_TXN,"ToolkitTransaction.rollback not implemented");
        ToolkitLog.logCritical(ToolkitLogConstants.CTX_TXN,"*******************************************");
        ToolkitLog.logTrace(ToolkitLogConstants.CTX_TXN,"END ToolkitTransaction.rollback()");
*/
    }


     /**
     * Returns the objects that changed during this transaction
     */
    public Collection getObjects() {
        Collection objs = new ArrayList();

        Iterator it = configurationObjects.keySet().iterator();
        while (it.hasNext()) {
            Object key = it.next();
            objs.add(configurationObjects.get(key));
        }

        return objs;

    }

    /**
     * Call to add an object to the set of objects that changed during this
     * transaction.
     * For the configuration process, this object will be
     * @see {ConfigurationModelContainer Configuration}
     * @param key is the id of the configuration
     * @param value is the configuration container
     */
    public void addObjects(Object key, Object value) {
        configurationObjects.put(key, value);

    }

     /**
     * Returns the objects that changed during this transaction.  For
     * the configuration process, these objects will be
     * @see {ConfigurationModelContainer Configurations}.
     * @return Collection of objects that changed during the transaction.
     */
    public Object getObject(Object key) {
        return configurationObjects.get(key);
    }

    public boolean contains(Object key) {
        return configurationObjects.containsKey(key);
    }

}
