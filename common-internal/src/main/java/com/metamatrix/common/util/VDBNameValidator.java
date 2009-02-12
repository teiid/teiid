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

package com.metamatrix.common.util;

import java.text.StringCharacterIterator;


/** 
 * @since 5.0
 * This class contains static methods used to validate VDB and Source names. It is not intented to be a fancy string
 * parser/checker, it is only intented for VDB name validation with the following rules.  
 * 
 * Rules:
 *      Names must contain only alphanumeric characters and _  (underscores)
 *      Names must begin with a alpha character
 *      
 *      Along with the above rules there are reserved words that cannot be used for VDB names:
 *          
 *          System
 *          Admin
 *          Help
 *          
 *      Along with the above rules there are reserved words that cannot be used for Source names:
 *          
 *          System
 *          
 *  
 */
public class VDBNameValidator {
    
    static String reservedVDBNames[] = {"System", "Admin", "Help"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    static String reservedSourceNames[] = {"System"}; //$NON-NLS-1$
    
    
    static public boolean isValid (String vdbName) {

        if (vdbName == null || vdbName.length() == 0) {
            return false;
        }
        
        if (containsInvalidChars(vdbName)) {
            return false;
        }
        
        // Check for Reserved words
        for (int i=0; i < reservedVDBNames.length; i++) {
            if (vdbName.equalsIgnoreCase(reservedVDBNames[i])) {
                return false;
            }
        }
        return true;
    }
    
    static public boolean isSourceValid (String sourceName) {

        if (sourceName == null || sourceName.length() == 0) {
            return false;
        }
        
        if (containsInvalidChars(sourceName)) {
            return false;
        }
        return !isSourceNameReserved(sourceName);
    }
    
    public static boolean isSourceNameReserved(String sourceName) {
        // Check for Reserved words
        for (int i=0; i < reservedSourceNames.length; i++) {
            if (sourceName.equalsIgnoreCase(reservedVDBNames[i])) {
                return true;
            }
        }
        return false;
    }


    private static boolean containsInvalidChars(String vdbName) {

        StringCharacterIterator charIterator = new StringCharacterIterator(vdbName);

        // Check to insure 1st character is a letter
        char c = charIterator.first();
        if (c != StringCharacterIterator.DONE) {
            if (!Character.isLetter(c)) {
                return true;   // first character must be a letter
            }   
        } else {
            return true;        // empty string is invalid, should never happen
        }

        while (true) {
            c = charIterator.next();
            if (c == StringCharacterIterator.DONE) {
                return false;
            }
            if (!Character.isLetterOrDigit(c) & (c != '_')) {
                return true;
            }
        }
    }
}
