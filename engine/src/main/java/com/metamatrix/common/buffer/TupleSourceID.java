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

package com.metamatrix.common.buffer;

import java.io.Serializable;

/**
 * Identifier for a tuple source.  The tuple source ID contains two
 * pieces of information: a unique ID (unique across locations) and
 * a location.  Both must be represented by a string but different
 * systems may use different location descriptions.  The
 * {@link com.metamatrix.common.buffer.BufferManagerLookup}
 * is the only party responsible for providing and decoding the location string.
 */
public class TupleSourceID implements Serializable {

	private static final String LOCATION_SEPARATOR = ":"; //$NON-NLS-1$

	private String idValue;
	private String location;

	/**
	 * Creates a tuple source ID given a string version of the ID - the
     * location should be encoded within the stringID, in the form
     * <location>:<idValue>.
     * @param stringID the String ID; may not be null (zero-length is tolerated)
	 */
	public TupleSourceID(String stringID){
        if(stringID == null){
        	throw new IllegalArgumentException("id may not be null"); //$NON-NLS-1$
        }
        
		int index = stringID.indexOf(LOCATION_SEPARATOR);
		if(index >= 0) {
			this.location = stringID.substring(0, index);
			this.idValue = stringID.substring(index+1);

		} else {
			this.location = null;
			this.idValue = stringID;
		}
	}

	/**
	 * Creates a tuple source ID given a string version of the ID
     * and the location.
     * @param idValue Unique ID; may not be null (zero-length is tolerated)
     * @param location Location string
	 */
	public TupleSourceID(String idValue, String location){
        if(idValue == null){
        	throw new IllegalArgumentException("id may not be null"); //$NON-NLS-1$
        }

		this.idValue = idValue;
		this.location = location;
	}

    /**
     * Get location string
     * @return the location; may be null
     */
	public String getLocation() {
		return this.location;
	}

    /**
     * Get unique ID value, does not contain location
     * @return ID value; never null but possibly zero-length
     */
	public String getIDValue() {
		return this.idValue;
	}

    /**
     * Get string combining the unique ID and location in the form
     * <location>:<idValue> if a location is specified.  Otherwise,
     * just the idValue is returned.
     */
    public String getStringID() {
    	if(this.location == null) {
    		return this.idValue;
    	}
	    return this.location + LOCATION_SEPARATOR + this.idValue;
    }

    /**
     * Compares two TupleSourceIDs for equality.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(obj == null || ! (obj instanceof TupleSourceID)) {
            return false;
        }

        TupleSourceID other = (TupleSourceID) obj;
        return other.getIDValue().equals(getIDValue());
    }

    /**
     * Get hash code
     * @return hash code
     */
    public int hashCode() {
        return this.idValue.hashCode();
    }

    /**
     * Get combined string representation of TupleSourceID
     * @return String representation
     */
    public String toString() {
    	return getStringID();
    }
}
