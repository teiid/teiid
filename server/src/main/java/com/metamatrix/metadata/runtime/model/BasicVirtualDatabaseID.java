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

package com.metamatrix.metadata.runtime.model;

import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.util.ErrorMessageKeys;

public class BasicVirtualDatabaseID extends BasicMetadataID implements VirtualDatabaseID {

    private String version;

/**
 * Call constructor to instantiate a VirtualDatabaseID object for the fully qualified Virtual Database name, version and an internal unique identifier.
 */
    public BasicVirtualDatabaseID(String fullName, String versionName, long internalUniqueID) {
        super(fullName, internalUniqueID);
        this.version = versionName;
        updateHashCode();
    }

/**
 * Call constructor to instantiate a VirtualDatabaseID object for the fully qualified Virtual Database name and version.
 */
    public BasicVirtualDatabaseID(String fullName, String versionName) {
        super(fullName);
        this.version = versionName;
        updateHashCode();
    }
/**
 * returns the version.
 * @return String
 */
    public String getVersion() {
	    return version;
    }

    public void setVersion(String version){
        this.version = version;
        updateHashCode();
    }

    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if ( this == obj ) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        //if ( this.getClass().isInstance(obj) ) {
        if ( obj instanceof BasicVirtualDatabaseID ) {

        	// Do quick hash code check first
        	if( this.hashCode() != obj.hashCode() ) {
        		return false;
		      }

            // If the types aren't the same, then fail
            BasicVirtualDatabaseID that = (BasicVirtualDatabaseID) obj;
            if ( this.getClass() != that.getClass() ) {
                return false;
            }

            return (this.getFullName() + version).equalsIgnoreCase( that.getFullName() + that.getVersion() );
        }

        // Otherwise not comparable ...
        return false;
    }

    public int compareTo(Object obj) {
        BasicVirtualDatabaseID that = (BasicVirtualDatabaseID) obj;     // May throw ClassCastException
        if ( obj == null ) {
            throw new MetaMatrixRuntimeException(ErrorMessageKeys.GEN_0005, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0005));
        }

        int diff = this.hashCode() - that.hashCode();
        if ( diff != 0 ) {
            return diff;
        }

        if ( this.getClass() != that.getClass() ) {
            diff = this.getClass().hashCode() - that.getClass().hashCode();
            return diff;
        }

        return (this.getFullName() + version).compareToIgnoreCase( that.getFullName() + that.getVersion());
    }

    public int compareToByName(Object obj) {
        BasicVirtualDatabaseID that = (BasicVirtualDatabaseID) obj;     // May throw ClassCastException
        if ( obj == null ) {
            throw new MetaMatrixRuntimeException(ErrorMessageKeys.GEN_0005, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.GEN_0005));
        }

        return (this.getFullName() + version).compareToIgnoreCase( that.getFullName() + that.getVersion());
    }

    protected int computeHashCode() {
        return (this.getFullName() + version).toLowerCase().hashCode();
    }
}

