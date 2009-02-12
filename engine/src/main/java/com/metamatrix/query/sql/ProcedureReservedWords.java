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

package com.metamatrix.query.sql;

import java.util.HashSet;
import java.util.Set;

/**
 * Special variable names in stored procedure language.
 */
public class ProcedureReservedWords {

    public static final String INPUT = "INPUT"; //$NON-NLS-1$

    public static final String ROWS_UPDATED = "ROWS_UPDATED"; //$NON-NLS-1$

	public static final String CHANGING = "CHANGING"; //$NON-NLS-1$

    public static final String VARIABLES = "VARIABLES"; //$NON-NLS-1$
    
    public static final String USING = "USING"; //$NON-NLS-1$

    public static final String[] ALL_WORDS = new String[] {
        INPUT,
        ROWS_UPDATED,
        CHANGING,
        VARIABLES,
        USING
    };        

    /**
     * Set of CAPITALIZED reserved words for checking whether a string is a reserved word.
     */
    private static final Set PROCEDURE_RESERVED_WORDS = new HashSet();

    // Initialize PROCEDURE_RESERVED_WORDS set
    static {
        // Iterate through the reserved words and capitalize all of them
        for ( int i=0; i!=ProcedureReservedWords.ALL_WORDS.length; ++i ) {
            String reservedWord = ProcedureReservedWords.ALL_WORDS[i];
            ProcedureReservedWords.PROCEDURE_RESERVED_WORDS.add( reservedWord.toUpperCase() );    
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
        return PROCEDURE_RESERVED_WORDS.contains(str.toUpperCase());    
    }
}
