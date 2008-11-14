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
import com.metamatrix.platform.config.transaction.ConfigTransaction;
import com.metamatrix.platform.config.transaction.ConfigTransactionLockFactory;

public class XMLConfigurationTransaction extends ConfigTransaction {
//	private static final String NON_TRANSACTION_ACQUIRED_BY = "ReadTransaction";

    private XMLConfigurationMgr configMgr;
    

    XMLConfigurationTransaction(ConfigTransactionLockFactory lockFactory, XMLConfigurationMgr mgr, TransactionID txnID, Object source, long defaultTimeoutSeconds) {
		super(lockFactory, txnID, source, defaultTimeoutSeconds);
		this.configMgr = mgr;
				
    }

    XMLConfigurationTransaction(ConfigTransactionLockFactory lockFactory, XMLConfigurationMgr mgr, TransactionID txnID, long defaultTimeoutSeconds) {
        super(lockFactory, txnID, defaultTimeoutSeconds);
        this.configMgr = mgr;
    }

        
     
    /**
     * Complete the transaction represented by this TransactionObject.
     * @throws TransactionException if the transaction is unable to commit.
     */
    public void commit() throws TransactionException{  
     	super.commit();
   	
		if (getStatus() == TransactionStatus.STATUS_COMMITTED) {
        	if ( isReadOnly() ) {           
            	return;
        	}
      		configMgr.applyTransaction(this);
        }
        
//		System.out.println("Committed Write ConfigTransaction for " + lock.getLockHolder());
        
    }
    
    public void rollback() throws TransactionException{
        try {
            super.rollback();
        } finally {
            configMgr.rollbackTransaction();
        }
    }    

    
}

