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

package com.metamatrix.modeler.internal.transformation.util;

import com.metamatrix.core.id.InvalidIDException;
import com.metamatrix.core.id.ObjectID;
import com.metamatrix.core.id.UUID;

public class UuidUtil {

	/** Delimiter character used when specifying fully qualified entity names */
	public static final char DELIMITER_CHAR = '.';

    /**
     * This is a string that is used to look for prefixed UUID strings.  The
     * Modeler can create and use queries that contain UUIDs rather than full name;
     * however, in cases where aliasing is used (e.g., "SELECT mmuuid:asdda... as A")
     * the "as A" gets lost later on in the query when "A" is used.  Therefore, the
     * Modeler will support prefixing the UUID strings with the alias
     * (e.g., "A.mmuuid:asdd...").  This constant is used to determine whether
     * a string contains a UUID prefixed by an alias.
     * <p>
     * Here's an example:
     * <pre>
     *   SELECT a.mmuuid:aaaaaa, b.mmuuid:bbbbbb, mmuuid:zzzzzz.mmuuid:cccccc
     *   FROM mmuuid:xxxxxx AS a, mmuuid:yyyyyy AS b, mmuuid:zzzzzz
     * </pre>
     * Here, the 'a.mmuid.aaaaaa' is treated as a fully-qualified element
     * (of the form Group.Element) where the group is defined as an alias
     * of one of the groups ('FROM mmuuid:xxxxx AS a').  This resolver does NOT
     * need to do anything with these fully-qualified names EXCEPT process
     * the UUID.
     */
    private static final String PREFIXED_OBJECT_ID_KEY = DELIMITER_CHAR + UUID.PROTOCOL + ObjectID.DELIMITER;
    private static final String PREFIXED_OBJECT_ID_KEY_UCASE = DELIMITER_CHAR + UUID.PROTOCOL_UCASE + ObjectID.DELIMITER;
    private static final String DELIMITED_PROTOCOL = UUID.PROTOCOL + ObjectID.DELIMITER;
    private static final String DELIMITED_PROTOCOL_UCASE = UUID.PROTOCOL_UCASE + ObjectID.DELIMITER;
    private static final int    DELIMITED_PROTOCOL_LENGTH = DELIMITED_PROTOCOL.length();

    public static boolean isStringifiedUUID( final String str ) {
		boolean result = false;

		try {
			String string = stripPrefixFromUUID(str);
			// strip the protocol before trying to determine if this a valid UUID
			int index = string.indexOf(DELIMITED_PROTOCOL);
			if(index == -1) {
				index = string.indexOf(DELIMITED_PROTOCOL_UCASE);
			}
			if(index != -1) {
				index = index + DELIMITED_PROTOCOL_LENGTH;
				string = string.substring(index);
				UUID.stringToObject(string);
				result = true;                
			} else {
				result = false;
			}   
		} catch ( InvalidIDException e ) {
			result = false;
		}
		return result;
	}    

	/**
	 * If the specified string contains an alias group in front
	 * of the UUID, strip off that prefix and return just
	 * the UUID.  If the string does NOT contain a prefixed UUID,
	 * this method simply returns the input string.
	 */
	public static String stripPrefixFromUUID( final String str ) {
		if ( str == null ) {
			return null;    
		} 
		// Look for a "xxx." preceding the ID protocol; if it is there,
		// then just remove it.  This prefix is there whenever a query
		// in the Modeler uses aliases to rename an element.
		int index = str.indexOf(PREFIXED_OBJECT_ID_KEY);    // returns index of '.' preceeding protocol
		if(index == -1) {
			index = str.indexOf(PREFIXED_OBJECT_ID_KEY_UCASE);
		}
		if ( index != -1 ) {
			return str.substring(index+1);
		}
		return str;
	}
}
