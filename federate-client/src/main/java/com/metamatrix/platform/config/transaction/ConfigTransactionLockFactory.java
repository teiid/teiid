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

import java.util.Date;
import java.util.Properties;

import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;


public abstract class ConfigTransactionLockFactory {

	/**
	 * Property, when defined, will determine the length of time after
	 * which a transaction lock will expire.
	 * Default is 30000 ms  or 50 seconds
	 */
	public static final String TRANSACTION_LOCK_LIMIT = "metamatrix.config.transaction.lock.expiration"; //$NON-NLS-1$


	/**
	 * This is a property that should be used only for testing or
	 * special single vm environments.  Developers can specify this
	 * option when running JUnit test.
	 */



	private static final long DEFAULT_LOCK_LIMIT = 60000; // "60000" or 60 seconds.
	private long lockTimeLimit = DEFAULT_LOCK_LIMIT;


	private Properties properties;


	protected ConfigTransactionLockFactory(Properties props)  {
		this.properties = (Properties) props.clone();

		String limit = props.getProperty(TRANSACTION_LOCK_LIMIT);
		if (limit != null && limit.trim().length() > 0) {
			lockTimeLimit = Long.parseLong(limit);
		}

	}

/**
 *
 * @param principal indicates who is requesting the lock
 * @param lockReason is why the lock is needed
 * @param force should be set to <code>true</code> if the lock should be obtained, regardless
 * @return ConfigTransactionLock is the lock indicating this principal now control
 * @throws ConfigTransactionException is thrown if a lock is already held by someone else and
 *          the force option is false
 * @throws ConfigTransactionLockException is thrown when the locking mechinism fails
 */
	public final synchronized ConfigTransactionLock obtainConfigTransactionLock(String principal, int lockReason, boolean force)
        throws ConfigTransactionException, ConfigTransactionLockException {

        ConfigTransactionLock currentLock = getCurrentLock();

        if (currentLock != null) {
            if (force) {
                    releaseLock();

            } else {
                Date t = new Date(currentLock.getTimeOfAcquisitionAsLong());
                ConfigTransactionException e = new ConfigTransactionException(ConfigMessages.CONFIG_0181,
                    ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0181,  new Object[]{principal, currentLock.getLockHolder(), t}));
                e.setTransactionState(ConfigTransactionException.TRANS_ALREADY_LOCKED);
                throw e;
            }
        }

            ConfigTransactionLock newLock = obtainLock(principal, lockReason);
            return newLock;

	}


/**
 * Releases the user lock, to make it available for someone else
 * @param userlock is the lock to be released.
 * @throws ConfigTransactionException is thrown is the userlock is not the same as the
 *      current lock in the system
 * @throws ConfigTransactionLockException is thrown when the locking mechinism fails
 */
    public final synchronized void releaseConfigTransactionLock(ConfigTransactionLock userlock)
    throws ConfigTransactionException, ConfigTransactionLockException {

        ConfigTransactionLock currentLock = getCurrentLock();
        if (currentLock.equals(userlock)) {
            releaseLock();
        } else {
            Date t = new Date(currentLock.getTimeOfAcquisitionAsLong());
            ConfigTransactionException e = new ConfigTransactionException(ConfigMessages.CONFIG_0182,
                    ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0182,  new Object[]{currentLock.getLockHolder(), t}));
            e.setTransactionState(ConfigTransactionException.TRANS_NOT_LOCKED_BY_SAME_USER);
            throw e;

        }

    }

/**
 * Returns the current lock in the system, a null object will be returned
 * if no lock current exists.
 * @return ConfigTransactionLock representing the current lock
 * @throws ConfigTransactionLockException is thrown when the locking mechinism fails
 */
    public final synchronized ConfigTransactionLock getCurrentConfigTransactionLock()
    throws ConfigTransactionLockException {

		return getCurrentLock();

    }


    /**
     * Helper method for creating the lock.  The constructor on the ConfigTransactionLock is protected
     * so that its creation is strictly controlled within this package.
     * @param principal
     * @param acquiredDateTime
     * @param lockExpiresAt
     * @param lockReason
     * @return
     */
    protected final ConfigTransactionLock createLock(String principal, long acquiredDateTime, long lockExpiresAt, int lockReason) {
		ConfigTransactionLock lock = new ConfigTransactionLock(principal, acquiredDateTime, lockExpiresAt, lockReason);
		return lock;


    }

	protected Properties getProperties() {
		return this.properties;
	}


	protected long getLockTimeLimit() {
		return this.lockTimeLimit;
	}

    protected final long calculateLockExpiration(long acquiredDate) {
    	return acquiredDate + getLockTimeLimit();
    }




	/**
	 * Will try to obtain a lock, if a lock doesn't already exist.
	 */
	protected abstract ConfigTransactionLock obtainLock(String principal, int lockReason)
    throws ConfigTransactionLockException;

	/**
	 * Releases the lock so that someone else can obtain a lock
	 */
	protected abstract void releaseLock() throws ConfigTransactionLockException ;

	/**
	 * Returns the lock currently held
	 */
	protected abstract ConfigTransactionLock getCurrentLock() throws ConfigTransactionLockException ;


	/**
	 * Called by the instantiator to enable the factory to initialize itself.
	 */
	public abstract void init() throws ConfigurationException;


}
