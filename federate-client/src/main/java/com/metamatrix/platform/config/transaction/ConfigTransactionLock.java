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

import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;

/**
 * Created on Sep 10, 2002
 *
 * The ConfigTransctionLock is used in conjuction with {@link ConfigUserTransaction ConfigUserTransaction}.
 * to ensure only one user can obtain a lock at one time.
 * In order to perform an update on a configuration a {@link ConfigUserTransaction ConfigUserTransaction} must
 * be obtained from the {@see ConfigTransactionLockFactory LockFactory}.
 */
public class ConfigTransactionLock {

	public static final int LOCK_SERVER_STARTING = 1;
	public static final int LOCK_CONFIG_CHANGING = 2;


 	private String lockHolder;
    private long lockAcquiredAt;
    private long lockExpiresAt;
    private int lockReason;


    /**
     * The hash code combines the lockHolder, lockReason and lockAcquiredAt to make
     * up the hashCode.  It will be set when the object is created.
     * this object is immutable, it should never have
     * to be updated.  It can be used for a fail-fast equals check (where unequal
     * hash codes signify unequal objects, but not necessarily vice versa) as well.
     */
    private int hashCode;



    ConfigTransactionLock(String lockHolder, long lockAcquiredAt, long lockExpiresAt, int reason ) {
        if(lockHolder == null){
            ArgCheck.isNotNull(lockHolder, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0163));
        }
        this.lockHolder = lockHolder;
        this.lockAcquiredAt = lockAcquiredAt;
        this.lockReason = reason;
        this.lockExpiresAt = lockExpiresAt;

        updateHashCode();
    }



    /**
     * Returns the name of the user holding the lock
     * @return String lock holder name
     */
    public String getLockHolder() {
        return lockHolder;
    }


    /**
     * Returns the lock acquisition date and time in <code>long</code> format
     * @return long date and time
     */
    public long getTimeOfAcquisitionAsLong() {
        return lockAcquiredAt;
    }


    /**
     * Returns the lock acquisition date and time in <code>Date</code> format
     * @return Date date and time
     */
    public Date getTimeOfAcquisition() {
        return new Date(lockAcquiredAt);
    }

    public long getTimeofLockExpirationAsLong() {
    	return this.lockExpiresAt;
    }

    public Date getTimeOfExpiration() {
    	return new Date(this.lockExpiresAt);
    }

    public boolean hasExpired() {
		if (getTimeTillExpires() > 0) {
			return false;
		}

		return true;
    }

    public long getTimeTillExpires() {
		long currentTime = System.currentTimeMillis();
		long expireTime = getTimeofLockExpirationAsLong();

		if (expireTime > currentTime) {
			long dif = expireTime - currentTime;
			return dif;
		}

		return 0;
    }

    public int getLockReasonCode() {
    	return lockReason;
    }

    public String getLockReasonDesc() {
    	switch(lockReason) {
    		case LOCK_SERVER_STARTING:
    			return "Locked for Serving Starting"; //$NON-NLS-1$

    		case LOCK_CONFIG_CHANGING:
    			return "Lock for Configuration Change"; //$NON-NLS-1$

    		default:
    			return "Lock Reason undefined for code " + lockReason; //$NON-NLS-1$
    	}

    }
    /**
     * Compares this object to another. If the specified object is an instance of
     * the same class, then this method compares the name; otherwise, it throws a
     * ClassCastException (as instances are comparable only to instances of the same
     * class).  Note:  this method is consistent with <code>equals()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {

        if ( obj == null ) {
            throw new IllegalArgumentException(ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0164));
        }
        int diff = 0;
        if ( this.getClass() != obj.getClass() ) {
            diff = this.getClass().hashCode() - obj.getClass().hashCode();
            return diff;
        }


        diff = this.hashCode() - obj.hashCode();
        if ( diff != 0 ) {
            return diff;
        }
        return 0;
    }
    /**
     * Returns true if the specified object is semantically equal to this instance.
     * Note:  this method is consistent with <code>compareTo()</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return whether the object is equal to this object.
     */
    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if ( this == obj ) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        //if ( this.getClass().isInstance(obj) ) {
        if ( obj instanceof ConfigTransactionLock ) {

			ConfigTransactionLock that = (ConfigTransactionLock) obj;
            // Do quick hash code check first
            if( this.getHashCode() != that.getHashCode() ) {
                return false;
            }
            // If the types aren't the same, then fail
            if ( this.getClass() != that.getClass() ) {
                return false;
            }
            return true;
        }

        // Otherwise not comparable ...
        return false;

    }

    protected final int getHashCode() {
    	return this.hashCode;
    }

    /**
     * Update the currently cached hash code value with a newly computed one.
     * This method should be called in any subclass method that updates any field.
     * This includes constructors.
     * <p>
     * The implementation of this method invokes the <code>computeHashCode</code>
     * method, which is likely overridden in subclasses.
     */
    protected final void updateHashCode() {
        this.hashCode = computeHashCode();
    }
    /**
     * Return a new hash code value for this instance.  If subclasses provide additional
     * and non-transient fields, they should override this method using the following
     * template:
     * <pre>
     * protected int computeHashCode() {
     *      int result = super.computeHashCode();
     *      result = HashCodeUtil.hashCode(result, ... );
     *      result = HashCodeUtil.hashCode(result, ... );
     *      return result;
     * }
     * </pre>
     * Any specialized implementation must <i>not<i> rely upon the <code>hashCode</code>
     * method.
     * <p>
     * Note that this method does not and cannot actually update the hash code value.
     * Rather, this method is called by the <code>updateHashCode</code> method.
     * @return the new hash code for this instance.
     */
    protected int computeHashCode() {
    	int hc = HashCodeUtil.hashCode(0, lockReason);
    	hc = HashCodeUtil.hashCode(hc, lockAcquiredAt);
    	hc = HashCodeUtil.hashCode(hc, lockHolder);

        return hc;
    }



    /**
     * Returns in displayable format the lockholder and the date acquired.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer("Configuration Locked Acquired by "); //$NON-NLS-1$
        sb.append(lockHolder);
        sb.append(" on "); //$NON-NLS-1$
        sb.append(new Date(lockAcquiredAt));
        sb.append(" reason "); //$NON-NLS-1$
        sb.append(getLockReasonDesc());

        return sb.toString();

    }
}
