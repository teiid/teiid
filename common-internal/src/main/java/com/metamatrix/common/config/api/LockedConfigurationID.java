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

package com.metamatrix.common.config.api;

import java.util.Date;

/**
 * This class acts as a wrapper around a single ConfigurationID, denoting that ID
 * as being locked.  However, this lock wrapper, since it extends ConfigurationID,
 * can be treated like a ConfigurationID.  In other words, this class uses the
 * Decorator pattern to add a locked attribute to any ConfigurationID instance.
 */
public class LockedConfigurationID extends ConfigurationID{

    private ConfigurationID originalID;
    private String version;
    private String lockHolder;
    private long lockAcquiredAt;

	/**
     * Create a locked wrapper around the specified MetadataID instance.
     * @param configrationName is the configuration that is to be locked.
     * @param versionName along with configurationName uniquely identifies the configuration
     * @throws IllegalArgumentException if the ID is null
     */
	public LockedConfigurationID( ConfigurationID id, String versionName, String lockHolder, long lockAcquiredAt ){
        // The following allows this class to NOT overload and forward
        // most methods, but does make a copy of the full name.
        super( ((id!=null)?id.getFullName():"")); //$NON-NLS-1$
 		if ( id == null || versionName == null || lockHolder == null ) {
			throw new IllegalArgumentException("A null parameter has been passed when identifying a locked configuration"); //$NON-NLS-1$
		}
		this.originalID = id;
        this.version = versionName;
        this.lockHolder = lockHolder;
        this.lockAcquiredAt = lockAcquiredAt;

    }

    public ConfigurationID getOriginalID() {
        return originalID;
    }

    public String getVersion() {
        return version;
    }

    public String getLockHolder() {
        return lockHolder;
    }

    public long getTimeOfAcquisitionAsLong() {
        return lockAcquiredAt;
    }

    public Date getTimeOfAcquisition() {
        return new Date(lockAcquiredAt);
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
        return this.originalID.compareTo(obj);
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
        if (this == obj) {
            return true;
        }
        return this.originalID.equals(obj);
    }
    /**
     * Return a deep cloned instance of this object.  Subclasses must override
     * this method.
     * @return the object that is the clone of this instance.
     * @throws CloneNotSupportedException if this object cannot be cloned (i.e., only objects in
     * {@link com.metamatrix.metadata.api.Defaults Defaults} cannot be cloned).
     */
    public synchronized Object clone() throws CloneNotSupportedException {
        Object result = null;
        try {
            result = new LockedConfigurationID(this.originalID, this.version, this.lockHolder, this.lockAcquiredAt );
        } catch ( Exception e ) {
        }
        return result;
    }
}

