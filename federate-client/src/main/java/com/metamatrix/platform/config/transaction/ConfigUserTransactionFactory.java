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

import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;

//implements UserTransactionFactory
public class ConfigUserTransactionFactory  {
    private ConfigTransactionManager mgr;
//	private ConfigTransactionLockFactory configLockFactory;

	/**
	 * Construct a factory that can be used to create read or write transactions.
	 */
    public ConfigUserTransactionFactory( ConfigTransactionFactory transFactory) {
        if(transFactory == null){
            Assertion.isNotNull(transFactory, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0045,"TransactionFactory")); //$NON-NLS-1$
        }

        this.mgr = new ConfigTransactionManager(transFactory);
//        Assertion.isNotNull(configLockFactory, "The TransactionLockFactory reference may not be null");

//        this.configLockFactory = transFactory.getTransactionLockFactory();


    }


    /**
     * Create a new instance of a UserTransaction that may be used to
     * read information.  Read transactions do not have a source object
     * associated with them (since they never directly modify data).
     * @return the new transaction object
     */
    public ConfigUserTransaction createReadTransaction(String principal) throws TransactionException {
        return new ConfigUserTransaction( null, true, mgr, principal);
    }


    /**
     * Create a new instance of a UserTransaction that may be used to
     * write and/or update information.  The transaction will <i>not</i> have a source object
     * associated with it.
     * @param principal the name to be associated with this transaction
     * @return the new transaction object
     */
    public ConfigUserTransaction createWriteTransaction(String principal) throws TransactionException {
		return new ConfigUserTransaction( null, false, mgr, principal);
/*
    	if (this.configLockFactory != null) {

    	}
    	throw new ConfigTransactionException("ConfigUserTransactionFactory was not instantiated to all the creation of write transactions.");
 */
    }

    /**
     * Create a new instance of a UserTransaction that may be used to
     * write and/or update information. The source object will be used for all events that are
     * fired as a result of or as a product of this transaction.
     * @param source the object that is considered to be the source of the transaction;
     * may be null
     * @param principal the name to be associated with this transaction
     * @return the new transaction object
     */
    public ConfigUserTransaction createWriteTransaction(Object source, String principal) throws TransactionException {
		return new ConfigUserTransaction( source, false, mgr, principal);
/*
    	if (this.configLockFactory != null) {

    	}
    	throw new ConfigTransactionException("ConfigUserTransactionFactory was not instantiated to all the creation of write transactions.");
*/
    }
}
