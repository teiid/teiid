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

package org.teiid.connector.jdbc.teradata;

import java.util.ArrayList;
import java.util.List;

import org.teiid.connector.jdbc.JDBCCapabilities;



/** 
 * for Teradata database Release V2R5.1
 */
public class TeradataCapabilities extends JDBCCapabilities {

    public TeradataCapabilities() {
    }
    
    /* 
     * @see com.metamatrix.data.ConnectorCapabilities#supportsFullOuterJoins()
     */
    public boolean supportsFullOuterJoins() {
        return false;
    }
    
    /**
     * @see com.metamatrix.data.ConnectorCapabilities#getSupportedFunctions()
     */
    public List getSupportedFunctions() {
        List supportedFunctions = new ArrayList();
        supportedFunctions.addAll(super.getSupportedFunctions());
        supportedFunctions.add("ABS"); //$NON-NLS-1$
        supportedFunctions.add("ACOS"); //$NON-NLS-1$
        supportedFunctions.add("ASIN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        supportedFunctions.add("COS"); //$NON-NLS-1$
        supportedFunctions.add("EXP"); //$NON-NLS-1$
//        supportedFunctions.add("LOG"); //$NON-NLS-1$ // "LN"
//        supportedFunctions.add("LOG10"); //$NON-NLS-1$ // "LOG"
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$
        supportedFunctions.add("||"); //$NON-NLS-1$
//        supportedFunctions.add("CONCAT"); //$NON-NLS-1$ // "||"
//        supportedFunctions.add("LCASE"); //$NON-NLS-1$ // "LOWER"
//        supportedFunctions.add("LOCATE"); //$NON-NLS-1$ //"POSITION", "INDEX" ?
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
//        supportedFunctions.add("UCASE"); //$NON-NLS-1$ // "UPPER"
        supportedFunctions.add("UPPER"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        supportedFunctions.add("DAY"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$

        supportedFunctions.add("CAST"); //$NON-NLS-1$
        //supportedFunctions.add("CONVERT"); //$NON-NLS-1$ "CAST"

        return supportedFunctions;
    }
    
}
