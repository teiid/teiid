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

package com.metamatrix.core.id;

import java.io.Serializable;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.util.Assertion;

/**
 * <p>This class represents a universally unique identifier, consisting of
 * two long integral values. </p> 
 *
 * <p>This identifier is supposed to be unique both spatially and temporally.
 * It is based on version 4 IETF variant random UUIDs. </p>
 */
public class UUID implements ObjectID, Serializable {

    private static final String NOT_UUID_MESSAGE = CorePlugin.Util.getString("UUID.ID_must_be_of_type_UUID_1"); //$NON-NLS-1$
    private static final String UNPARSABLE_MESSAGE = CorePlugin.Util.getString("UUID.ID_must_be_of_type_UUID_to_parse_2"); //$NON-NLS-1$
    /**
     * The variants allowed by the UUID specification.
     */
    class Variant {
        public static final int NSC_COMPATIBLE    = 0;
        public static final int STANDARD          = 2;
        public static final int MICROSOFT         = 6;
        public static final int RESERVED_FUTURE   = 7;
    }

    class Version {
        public static final int TIME_BASED        = 1;
        public static final int DCE_RESERVED      = 2;
        public static final int NAME_BASED        = 3;
        public static final int PSEUDO_RANDOM     = 4;
    }

    public static final String PROTOCOL = "mmuuid"; //$NON-NLS-1$
    public static final String PROTOCOL_UCASE = PROTOCOL.toUpperCase();
    private static final int ID_STRING_LEN = 36;
    public static final int FQ_LENGTH = PROTOCOL.length() + 1 + ID_STRING_LEN;

    private final java.util.UUID uuid;
    private String cachedExportableFormUuidString;

    /**
     * Construct an instance of this class from two long integral values.
     * Both values must be non-negative.
     * @throws IllegalArgumentException if either value is negative
     */
    public UUID( long mostSig, long leastSig ) {
        this.uuid = new java.util.UUID(mostSig, leastSig);
    }
    
    public UUID(java.util.UUID uuid) {
    	this.uuid = uuid;
    }
    
    /**
     * Return the first part of the UUID as a long.
     * @return first part of the UUID as a long
     */
    public static long getPart1(ObjectID id) {
    	Assertion.assertTrue((id instanceof UUID), UNPARSABLE_MESSAGE);
    	
    	UUID uuid = (UUID)id;
	    return uuid.uuid.getMostSignificantBits();
    }
    
    /**
     * Return the first part of the UUID as a long.
     * @return first part of the UUID as a long
     */
    public static long getPart2(ObjectID id) {
    	Assertion.assertTrue((id instanceof UUID), UNPARSABLE_MESSAGE);
    	
    	UUID uuid = (UUID)id;
    	return uuid.uuid.getLeastSignificantBits();
    }
    
    public static int getVariant(ObjectID id ) {
        Assertion.assertTrue((id instanceof UUID), NOT_UUID_MESSAGE);
        
        UUID uuid = (UUID)id;
        return uuid.uuid.variant();
    }

    public static int getVersion(ObjectID id ) {
        Assertion.assertTrue((id instanceof UUID), NOT_UUID_MESSAGE);
        
        UUID uuid = (UUID)id;
        return uuid.uuid.version();
    }

    /**
     * Return the name of the protocol that this factory uses.
     * @return the protocol name
     */
    public String getProtocol() {
	    return PROTOCOL;
    }
    
    /**
     * Returns the hashcode for this instance.  All of the bits in each 32-bit
     * part of the longs are exclusively 'or'd together to yield the hashcode.
     */
    public int hashCode() {
	    return uuid.hashCode();
    }

    /**
     * <p>Returns true if the specified object is semantically equal to this
     * instance.  Note:  this method is consistent with <code>compareTo()
     * </code>. </p>
     * <p>UUID instances are equal if they represent the same 128-bit value. </p>
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
        if ( obj instanceof UUID ) {
            UUID that = (UUID) obj;
            return this.uuid.equals(that.uuid);
		}

        // Otherwise not comparable ...
        return false;
    }
    
    /**
     * <p>Compares this object to another. If the specified object is 
     * not an instance of the LongID class, then this method throws a
     * ClassCastException (as instances are comparable only to instances
     * of the same class). </p>
     *
     * <p>Note:  this method <i>is</i> consistent with <code>equals()</code>,
     * meaning that <code>(compare(x, y)==0) == (x.equals(y))</code>. </p>
     *
     * @param obj the object that this instance is to be compared to; may not be null.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public int compareTo(Object obj) {
        UUID that = (UUID) obj;     // May throw ClassCastException
        return this.uuid.compareTo(that.uuid);
    }

    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public String toString(){
        return toString(ObjectID.DELIMITER);
    }

    /**
     * @see com.metamatrix.core.id.ObjectID#toString(char)
     */
    public String toString(char delim) {
        return new StringBuffer(43).append(PROTOCOL).append(delim).append(this.exportableForm()).toString();
    }

    /**
     * <p>Returns a 36-character string of six fields separated by hyphens,
     * with each field represented in lowercase hexadecimal with the same
     * number of digits as in the field. The order of fields is: time_low,
     * time_mid, version and time_hi treated as a single field, variant and
     * clock_seq treated as a single field, and node. </p>
     *
     * @return A string of the form 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
     *         where all the characters are lowercase hexadecimal digits
     */
    public String exportableForm() {
    	if (this.cachedExportableFormUuidString == null) {
    		this.cachedExportableFormUuidString = this.uuid.toString();
    	}
        return this.cachedExportableFormUuidString;
    }

    /**
     * Attempt to convert the specified string to the appropriate ObjectID instance.
     * @param value the stringified id with the protocol and ObjectID.DELIMITER already
     * removed, and which is never null or zero length
     * @return the ObjectID instance for the stringified ID if this factory is able
     * to parse the string, or null if the factory is unaware of the specified format.
     */
    public static ObjectID stringToObject(String value) throws InvalidIDException {
        try {
            return new UUID(java.util.UUID.fromString(value));
        } catch ( IllegalArgumentException e ) {
            throw new InvalidIDException(
                CorePlugin.Util.getString("UUID.InvalidFormatForProtocol",value,PROTOCOL)); //$NON-NLS-1$
        }
    }

}
