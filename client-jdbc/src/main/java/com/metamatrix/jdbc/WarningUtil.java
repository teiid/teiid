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

package com.metamatrix.jdbc;

import java.sql.SQLWarning;
import java.util.List;

import com.metamatrix.dqp.exception.SourceWarning;
import com.metamatrix.jdbc.api.PartialResultsWarning;

/**
 * Utilities for creating SQLWarnings.  
 */
class WarningUtil {

    private WarningUtil() {
    }
    
    /**
     * Used to wrap metamatrix warnings/exceptions into SQLWarning.
     * The metamatrix chain of warnings is translated into a chain of SQLWarnings.
     * @param reason String object which is the description of the warning.
     * @param ex Throwable object which needs to be wrapped.
     */
    static SQLWarning createWarning(Throwable ex) {
        if(ex instanceof SourceWarning) {
        	SourceWarning exception = (SourceWarning)ex;
        	if (exception.isPartialResultsError()) {
        		PartialResultsWarning warning = new PartialResultsWarning(JDBCPlugin.Util.getString("WarningUtil.Failures_occurred")); //$NON-NLS-1$
        		warning.addConnectorFailure(exception.getConnectorBindingName(), MMSQLException.create(exception));
        		return warning;
        	}
        }
        //## JDBC4.0-begin ##
        return new SQLWarning(ex);
        //## JDBC4.0-end ##
		/*## JDBC3.0-JDK1.5-begin ##
		return new SQLWarning(ex.getMessage()); 
	      ## JDBC3.0-JDK1.5-end ##*/
        
    }

    /**
     * Convert a List of warnings from the server into a single SQLWarning chain.
     * @param exceptions List of exceptions from server
     * @return Chain of SQLWarning corresponding to list of exceptions
     */
    static SQLWarning convertWarnings(List<Exception> exceptions) {
        if(exceptions == null || exceptions.size() == 0) {
            return null;    
        }
        SQLWarning warning = null;

        for (Exception ex : exceptions) {
            SQLWarning newWarning = createWarning(ex); 
            if(warning == null) {
                warning = newWarning;
            } else {
                warning.setNextWarning(newWarning);
            }
        }
     
        return warning;   
    }
}
