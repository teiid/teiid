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

package com.metamatrix.platform.config.spi.xml;

import com.metamatrix.common.id.TransactionIDFactory;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.core.id.ObjectIDFactory;

//implements UserTransactionFactory
public class ConfigUserTransactionFactory  {
    private ObjectIDFactory idFactory = new TransactionIDFactory();
//	private ConfigTransactionLockFactory configLockFactory;

	/**
	 * Construct a factory that can be used to create read or write transactions.
	 */
    public ConfigUserTransactionFactory() {
    }


    /**
     * Create a new instance of a UserTransaction that may be used to
     * read information.  Read transactions do not have a source object
     * associated with them (since they never directly modify data).
     * @return the new transaction object
     */
    public ConfigUserTransaction createReadTransaction(String principal) throws TransactionException {
        return new ConfigUserTransaction( true, idFactory, principal);
    }


    /**
     * Create a new instance of a UserTransaction that may be used to
     * write and/or update information.  The transaction will <i>not</i> have a source object
     * associated with it.
     * @param principal the name to be associated with this transaction
     * @return the new transaction object
     */
    public ConfigUserTransaction createWriteTransaction(String principal) throws TransactionException {
		return new ConfigUserTransaction( false, idFactory, principal);
/*
    	if (this.configLockFactory != null) {

    	}
    	throw new ConfigTransactionException("ConfigUserTransactionFactory was not instantiated to all the creation of write transactions.");
 */
    }

}
