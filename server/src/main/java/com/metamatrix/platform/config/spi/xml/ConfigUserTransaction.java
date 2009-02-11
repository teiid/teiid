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

import com.metamatrix.common.id.TransactionID;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.TransactionStatus;
import com.metamatrix.core.id.ObjectIDFactory;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;

public class ConfigUserTransaction {
	
    private static final int DEFAULT_TIMEOUT = 300000; // 5 mins
	
	public static final int LOCK_SERVER_STARTING = 1;
	public static final int LOCK_CONFIG_CHANGING = 2;

    /** The underlying "real" transaction to which this user transaction is bound */
    private ConfigTransaction txn;

    /** Used to track the status when this user transaction is not bound to an underlying transaction */
    private int status;

    private boolean readTxn;

    private boolean alreadyBegun;
//	private ConfigTransactionLockFactory configLockFactory;
    private String name;
    
    private ObjectIDFactory idFactory;

    ConfigUserTransaction(boolean isReadOnly, ObjectIDFactory idFactory, String name )  {
        ArgCheck.isNotNull(idFactory);
        this.readTxn = isReadOnly;
        this.status = TransactionStatus.STATUS_UNKNOWN;
        this.idFactory = idFactory;
        this.name = name;
//        this.configLockFactory = null;


    }

    public void begin() throws TransactionException {
        if ( alreadyBegun ) {
            throw new IllegalStateException("already begun"); //$NON-NLS-1$
        }
        if ( this.status != TransactionStatus.STATUS_UNKNOWN ) {
            throw new TransactionException(ConfigMessages.CONFIG_0073, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0073));
        }
        
        TransactionID id = null;
        synchronized (idFactory) {
            id = (TransactionID) this.idFactory.create();
		}
        this.txn = new XMLConfigurationTransaction(XMLConfigurationMgr.getInstance(), id, DEFAULT_TIMEOUT);
        this.txn.begin(name, LOCK_CONFIG_CHANGING, this.readTxn);

// this needs to be last so that it officially has been begun until this step has completed
// also, its so commit cannot be called until the begin has completed.
		this.alreadyBegun = true;

//System.out.println("<CFG USER TRANS>ENDED BEGIN TRANS");

    }
    public void commit() throws TransactionException {
        if ( ! alreadyBegun ) {
            throw new TransactionException(ConfigMessages.CONFIG_0075, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0075));
        }
        if ( this.txn == null ) {
            throw new TransactionException(ConfigMessages.CONFIG_0076, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0076));
        }

        try {
        	this.txn.commit();
        } finally {
                // Get status so we always have it
	        this.status = this.txn.getStatus();
	        this.txn = null;        // we're done with the underlying transaction
        }
    }

    public void rollback() throws TransactionException {
        if ( ! alreadyBegun ) {
            return;     // there's nothing to do, 'cause this was likely caused after 'begin' failed
        }
        if ( this.txn == null ) {
            throw new TransactionException(ConfigMessages.CONFIG_0076, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0076));
        }
        try {
        	this.txn.rollback();
        } finally {
	        // Get status so we always have it
	        this.status = this.txn.getStatus();
	        this.txn = null;        // we're done with the underlying transaction
        }
    }

    public boolean isReadTransaction() {
    	return readTxn;
    }


    public ConfigTransaction getTransaction() {
    	return txn;
    }

}

