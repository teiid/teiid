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

package com.metamatrix.platform.config.transaction.lock.singlevm;

import java.util.Date;
import java.util.Properties;

import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;
import com.metamatrix.platform.config.transaction.ConfigTransactionException;
import com.metamatrix.platform.config.transaction.ConfigTransactionLock;
import com.metamatrix.platform.config.transaction.ConfigTransactionLockException;
import com.metamatrix.platform.config.transaction.ConfigTransactionLockFactory;

/**
 * @author vhalbert
 * Date Oct 9, 2002
 *
 */
public class SingleVMTransactionLockFactory extends  ConfigTransactionLockFactory {

	private static SingleVMTransactionLockFactory factory = null;

	private static ConfigTransactionLock lock = null;


	public SingleVMTransactionLockFactory(Properties props)  {
			super(props);
	}

	public void init() throws ConfigurationException {
			factory = new SingleVMTransactionLockFactory(getProperties());
	}

    private synchronized SingleVMTransactionLockFactory getInstance() {
    	return factory;
    }

    // implemented abstract method
	protected ConfigTransactionLock obtainLock(String principal, int lockReason) throws ConfigTransactionLockException {

		SingleVMTransactionLockFactory factory = getInstance();

		synchronized(factory) {
			return factory.obtainVMLock(principal, lockReason);
		}


	}

	protected ConfigTransactionLock getCurrentLock() throws ConfigTransactionLockException {

		return SingleVMTransactionLockFactory.lock;
	}

	private ConfigTransactionLock obtainVMLock(String principal, int lockReason) throws ConfigTransactionLockException {

		if (lock != null) {
            ConfigTransactionLockException e = new ConfigTransactionLockException(ConfigMessages.CONFIG_0184,
				ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0184,  principal, lock.getLockHolder()));

            e.setTransactionState(ConfigTransactionException.TRANS_ALREADY_LOCKED);
            throw e;
		}

		Date acquired = new Date();
		long acquiredTime = acquired.getTime();
		long expires = this.calculateLockExpiration(acquiredTime);

		ConfigTransactionLock newLock = super.createLock(principal, acquiredTime, expires,  lockReason);

		lock = newLock;

//		System.out.println("<CONFIGLOCK>Obtained lock for " + principal);
		return lock;

	}


	// implemented abstract method
    protected void releaseLock() throws ConfigTransactionLockException {
//			System.out.println("<CONFIGRELEASELOCK>checking to release lock for " + userlock.getLockHolder());
		SingleVMTransactionLockFactory factory = getInstance();

		synchronized(factory) {
			factory.releaseVMLock();
		}

    }



    private void releaseVMLock() throws ConfigTransactionLockException {

	    	lock = null;
//			System.out.println("<CONFIGRELEASELOCK>Released lock for " + userlock.getLockHolder());

	    	return;
    }

}
