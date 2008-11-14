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
    public class Variant {
        public static final int NSC_COMPATIBLE    = 0x0000;
        public static final int STANDARD          = 0x8000;
        public static final int MICROSOFT         = 0xc000;
        public static final int RESERVED_FUTURE   = 0xe000;
    }

    public class Version {
        public static final int TIME_BASED        = 1;
        public static final int DCE_RESERVED      = 2;
        public static final int NAME_BASED        = 3;
        public static final int PSEUDO_RANDOM     = 4;
    }

    public static final String PROTOCOL = "mmuuid"; //$NON-NLS-1$
    public static final String PROTOCOL_UCASE = PROTOCOL.toUpperCase();
    private static final char INTERNAL_DELIM = '-';
    private static final int BASE_16 = 16;
    private static final int ID_STRING_LEN = 36;
    public static final int FQ_LENGTH = PROTOCOL.length() + 1 + ID_STRING_LEN;

    private final long mostSig;
    private final long leastSig;
    private String cachedExportableFormUuidString;

    /**
     * Construct an instance of this class from two long integral values.
     * Both values must be non-negative.
     * @throws IllegalArgumentException if either value is negative
     */
    public UUID( long mostSig, long leastSig ) {
        //if ( mostSig < 0 || leastSig < 0 ) {
        //    throw new IllegalArgumentException(
        //        "The parts of the UUID must be unsigned: (" + mostSig
        //        + "," + leastSig + ")" );
        //}
        this.mostSig = mostSig;
        this.leastSig = leastSig;
    }
    
    /**
     * Return the first part of the UUID as a long.
     * @return first part of the UUID as a long
     */
    public static long getPart1(ObjectID id) {
    	Assertion.assertTrue((id instanceof UUID), UNPARSABLE_MESSAGE);
    	
    	UUID uuid = (UUID)id;
	    return uuid.mostSig;
    }
    
    /**
     * Return the first part of the UUID as a long.
     * @return first part of the UUID as a long
     */
    public static long getPart2(ObjectID id) {
    	Assertion.assertTrue((id instanceof UUID), UNPARSABLE_MESSAGE);
    	
    	UUID uuid = (UUID)id;
	    return uuid.leastSig;
    }
    
    public static int getVariant(ObjectID id ) {
        Assertion.assertTrue((id instanceof UUID), NOT_UUID_MESSAGE);
        
        UUID uuid = (UUID)id;
        return (int)((uuid.leastSig & 0xc000000000000000L) >>> (12*4));
    }

    public static int getVersion(ObjectID id ) {
        Assertion.assertTrue((id instanceof UUID), NOT_UUID_MESSAGE);
        
        UUID uuid = (UUID)id;
        return (int)((uuid.mostSig  & 0x000000000000f000L) >>> (3*4));
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
	    return (int)((mostSig >> 32) ^ mostSig ^ (leastSig >> 32) ^ leastSig);
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
        //if ( this.getClass().isInstance(obj) ) {
        if ( obj instanceof UUID ) {
            UUID that = (UUID) obj;
            return (this.mostSig == that.mostSig && this.leastSig == that.leastSig);
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
        Assertion.isNotNull(obj);

        long diff = this.mostSig - that.mostSig;
        if ( diff < 0 ) {
            return -1;
        }
        if ( diff > 0 ) {
            return 1;
        }

        diff = this.leastSig - that.leastSig;
        if ( diff < 0 ) {
            return -1;
        }
        if ( diff > 0 ) {
            return 1;
        }
        return 0;
    }

    /**
     * <p>Return whether the specified ObjectID instance is valid.  Only ObjectID
     * instances that are for this protocol will be passed in. </p>
     * 
     * <p>This implementation chekcs that the variant field holds the bits '10'
     * and that the version field holds . </p>
     *
     * @param id the ID that is to be validated, and which is never null
     * @return true if the instance is valid for this protocol, or false if
     * it is not valid.
     */
    public static boolean validate( ObjectID id ) {
        if ( id instanceof UUID ) {
            UUID uuid = (UUID)id;
//            // Check that variant bits are '10' and that version bits are '0001'
//            return ((uuid.leastSig & 0xc000000000000000L) == 0x8000000000000000L) &&
//                   ((uuid.mostSig  & 0x000000000000f000L) == 0x0000000000001000L);

            // Check the variant and the version ...
            return ( getVariant(uuid) == UUID.Variant.STANDARD &&
                     getVersion(uuid) == UUID.Version.TIME_BASED );
        }
        return false;
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
        // Tested performance of using the StringBuffer initialized to "43" versus just using the String concat
        // PROTOCOL + delim + this.exportableForm()
        // For 2,000,000 calls the results were:
        // String Concat = 1.7 seconds
        // StringBuffer  = 1.25 seconds
        // Result:  USE STRING BUFFER
        return new StringBuffer(43).append(PROTOCOL).append(delim).append(this.exportableForm()).toString();
    }

    /**
     * Returns a string representing the current state of the object used
     * for internal testing and debugging.
     * @return the string representation of this instance.
     */
    public String toStringTest() {
        return this.exportableForm()
            + '(' + this.mostSig + ',' + this.leastSig + ')';
    }
    
    /**
     * Helper method for test program.  This method takes a UUID, converts
     * it to "exportable" (String) format, then converts it back to a UUID,
     * and compares that UUID with the original.
     *
     * @param id1 The ID to test
     * @return False if the original and the created UUID after the back-and
     *         forth conversion do not match.
     */
    public static boolean testStringToObject( UUID id1 ) {
        String uuidString = id1.exportableForm();

        UUID id2 = null;
        try {
            id2 = (UUID)UUID.stringToObject( uuidString );
        } catch ( InvalidIDException e ) {
            e.printStackTrace();
        }

        boolean match = true;
        if ( id1.mostSig != id2.mostSig ) {
//            System.out.println("Most sig part of " + id1 + " does NOT match.");
            match = false;
        }
        if ( id1.leastSig != id2.leastSig ) {
//            System.out.println("Least sig part of " + id1 + " does NOT match.");
            match = false;
        }
        return match;
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
        // Create this ONCE and cache it.
        if( cachedExportableFormUuidString == null ) {
            cachedExportableFormUuidString = 
                           (toHex(mostSig  >> 32,  8)   + INTERNAL_DELIM +
                            toHex(mostSig  >> 16,  4)   + INTERNAL_DELIM +
                            toHex(mostSig       ,  4)   + INTERNAL_DELIM +
                            toHex(leastSig >> 48,  4)   + INTERNAL_DELIM +
                            toHex(leastSig      , 12) );
        } 
        return cachedExportableFormUuidString;
    }

    /**
     * <p>Returns a long value represented in hexadecimal form by the specified
     * number of hex digits. </p>
     * @param val The long value, to be converted to hex form
     * @param digits The number of hex digits we want
     * @return The original value in hex form, to the specified # digits
     */
    private static String toHex(long val, int digits) {
        // The following algorithm pads the result using bit arithmetic
        // rather than String padding.
        // Example:
        //    mask            100000000
        //    value                 101
        //    v=value&(mask-1) 00000101
        //    mask|v          100000101
        // This yields the string "100000101", which when converted to hexadecimal
        // format is '1??' (always starts with '1', followed by digits we care about).
        // We take the substring starting after the first '1', yielding the desired
        // hexadecimal value for '00000101'.

        // Create a mask that is 1 bit beyond the bounds of # hex digits we want
	    long hi = 1L << (digits * 4);   // 4 bits for every hex digit
	    // Logical 'or' of mask and "padded" value
	    return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }

    /**
     * Attempt to convert the specified string to the appropriate ObjectID instance.
     * @param value the stringified id with the protocol and ObjectID.DELIMITER already
     * removed, and which is never null or zero length
     * @return the ObjectID instance for the stringified ID if this factory is able
     * to parse the string, or null if the factory is unaware of the specified format.
     */
    public static ObjectID stringToObject(String value) throws InvalidIDException {

        if ( value.length() != ID_STRING_LEN ) {
            throw new InvalidIDException(
                CorePlugin.Util.getString("UUID.InvalidLengthForProtocol",value,PROTOCOL)); //$NON-NLS-1$
        }
        try {
            long part1 = Long.parseLong(value.substring( 0, 8),BASE_16);
            long part2 = Long.parseLong(value.substring( 9,13),BASE_16);
            long part3 = Long.parseLong(value.substring(14,18),BASE_16);
            long part4 = Long.parseLong(value.substring(19,23),BASE_16);
            long part5 = Long.parseLong(value.substring(24)   ,BASE_16);
            long most  = part1 << 32 | part2 << 16 | part3;
            long least = part4 << 48 | part5;
            return new UUID(most,least);
        } catch ( NumberFormatException e ) {
            throw new InvalidIDException(
                CorePlugin.Util.getString("UUID.InvalidFormatForProtocol",value,PROTOCOL)); //$NON-NLS-1$
        }
    }

}
