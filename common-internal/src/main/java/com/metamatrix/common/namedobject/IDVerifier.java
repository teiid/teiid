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

package com.metamatrix.common.namedobject;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * This class contains several helper methods that check the validity of
 * BaseID instances.
 * <p>
 * A BaseID is considered valid only if <i>all</i> of the following conditions are met:
 * <li>The full name is at least one character long;</li>
 * <li>The first character of the full name is a letter; and</li>
 * <li>Each of the remaining characters of the full name is either is a letter,
 * the MetadataID.DELIMITER_CHARACTER, a decimal digit, or the underscore
 * character ('_').
 */
public class IDVerifier {

    /**
     * The underscore character allowed in full names anywhere except for the first character.
     */
    public static final char UNDERSCORE_CHARACTER = '_';

    /**
     * The private character (in the form of char) that delimits the atomic components of the name
     */
	public static final char DELIMITER_CHARACTER = '.';

    /**
     * The result of a <code>performCheck</code> signifying that the full name is valid.
     */
    public static final int NONE                               = 0;

    /**
     * The result of a <code>performCheck</code> signifying that the full name was zero-length.
     */
    public static final int ZERO_LENGTH_FULL_NAME              = 1;

    /**
     * The result of a <code>performCheck</code> signifying that the first character of the
     * full name is not a letter.
     */
    public static final int FIRST_CHARACTER_IS_NOT_A_LETTER    = 2;

    /**
     * The result of a <code>performCheck</code> signifying that a character (other than
     * the first character) contained a character that wasn't a letter, digit, underscore,
     * space, or delimiter.
     */
    public static final int CONTAINS_INVALID_CHARACTER         = 4;

    /**
     * The result of a <code>performCheck</code> signifying that a character (other than
     * the first character) contained a character that was a space.
     */
    public static final int CONTAINS_SPACE                     = 8;

    /**
     * The result of a <code>performCheck</code> signifying that a character (other than
     * the first character) contained a character that was a space.
     */
    public static final int ALL                                = 15;

    /**
     * Method to determine whether a BaseID has a valid full name.  This method
     * is primarily a utility method used within the Configuration Service; it is anticipated
     * that this method does not need to be used by other components, since all
     * components (other than those within Metadata Service) are able to create
     * only valid IDs.
     * @param id the BaseID that is to be checked for validity
     * @return true if the specified BaseID is valid, or false otherwise
     * @throws IllegalArgumentException if the BaseID reference is null.
     */
    public static boolean isValid( BaseID id ) {
        if ( id == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.NAMEDOBJECT_ERR_0006));
        }
        return IDVerifier.performCheck( id.getFullName() ) == NONE;
    }

    /**
     * Method to determine whether a MetadataID has a valid full name, and, if not valid,
     * to identify why it is invalid.  This method
     * is primarily a utility method used within the Metadata Service; it is anticipated
     * that this method does not need to be used by other components, since all
     * components (other than those within Metadata Service) are able to create
     * only valid IDs.
     * @param id the BaseID that is to be checked for validity
     * @return true if the specified MetadataID is valid, or false otherwise
     * @throws IllegalArgumentException if the MetadataID reference is null.
     */
    public static int performCheck( BaseID id ) {
        if ( id == null ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.NAMEDOBJECT_ERR_0007));
        }
        return IDVerifier.performCheck( id.getFullName() );
    }

    public static int performCheck( String fullName ) {
        return performCheck(fullName,ALL);
    }

    public static int performCheck( String fullName, int levels ) {
        // This method is somewhat optimized to reduce recurrent checks,
        // and is therefore more complicated ...

        if ( fullName.length() == 0 && (levels & ZERO_LENGTH_FULL_NAME) != 0 ) {
            return ZERO_LENGTH_FULL_NAME;
        }
        StringCharacterIterator iter = new StringCharacterIterator(fullName);
        char c = iter.first();

        // If the first character should be a letter ...
        if ( (levels & FIRST_CHARACTER_IS_NOT_A_LETTER) != 0 && ! Character.isLetter(c) ) {
            return FIRST_CHARACTER_IS_NOT_A_LETTER;
        }

        // If the spaces are to be exclued ...
        if ( (levels & CONTAINS_SPACE) != 0 ) {

            // No spaces allowed and no invalid characters allowed ...
            if ( (levels & CONTAINS_INVALID_CHARACTER) != 0 ) {
                do {
                    if ( Character.isSpaceChar(c) ) {
                        return CONTAINS_SPACE;
                    } else if ( ! Character.isLetterOrDigit(c) && c != UNDERSCORE_CHARACTER && c != DELIMITER_CHARACTER ) {
                        return CONTAINS_INVALID_CHARACTER;
                    }
                } while ( (c=iter.next()) != CharacterIterator.DONE );
            }

            // Otherwise only spaces are not allowed ...
            else {
                return ( fullName.indexOf(' ') != -1 ) ? CONTAINS_SPACE : NONE;
            }
        }

        // Therefore spaces are allowed, but if invalid characters are not
        else if ( (levels & CONTAINS_INVALID_CHARACTER) != 0 ) {
            do {
                if ( ! Character.isLetterOrDigit(c) && ! Character.isSpaceChar(c) && c != UNDERSCORE_CHARACTER && c != DELIMITER_CHARACTER ) {
                    return CONTAINS_INVALID_CHARACTER;
                }
            } while ( (c=iter.next()) != CharacterIterator.DONE );
        }

        // Otherwise, spaces are allowed and invalid characters are as well

        return NONE;
    }

}




