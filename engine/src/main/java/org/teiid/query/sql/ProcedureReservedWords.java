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

package org.teiid.query.sql;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.teiid.language.SQLConstants;

/**
 * Special variable names in stored procedure language.
 */
public class ProcedureReservedWords {

    public static final String ROWCOUNT = "ROWCOUNT"; //$NON-NLS-1$

	public static final String CHANGING = "CHANGING"; //$NON-NLS-1$

    public static final String VARIABLES = "VARIABLES"; //$NON-NLS-1$
    
    public static final String DVARS = "DVARS"; //$NON-NLS-1$
    
    /**
 	 * Set of CAPITALIZED reserved words for checking whether a string is a reserved word.
 	 */
    private static final Set<String> RESERVED_WORDS = new HashSet<String>();

    // Initialize RESERVED_WORDS set - This is a poor man's enum.  To much legacy code expects the constants to be Strings.
 	static {
 		Field[] fields = SQLConstants.class.getDeclaredFields();
 		for (Field field : fields) {
 			if (field.getType() == String.class) {
 				try {
					RESERVED_WORDS.add((String)field.get(null));
				} catch (Exception e) {
				}
 			}
 		}
 	}

    /** Can't construct */
    private ProcedureReservedWords() {}

    /**
     * Check whether a string is a procedure reserved word.  
     * @param str String to check
     * @return True if procedure reserved word, false if not or null
     */
    public static final boolean isProcedureReservedWord(String str) {
        if (str == null) { 
            return false;    
        }
        return RESERVED_WORDS.contains(str.toUpperCase());    
    }
}
