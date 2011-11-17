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

package org.teiid.jdbc;

import java.sql.SQLWarning;
import java.util.List;

import org.teiid.client.SourceWarning;
import org.teiid.core.TeiidException;



/**
 * Utilities for creating SQLWarnings.  
 */
class WarningUtil {

    private WarningUtil() {
    }
    
    /**
     * Used to wrap warnings/exceptions into SQLWarning.
     * The chain of warnings is translated into a chain of SQLWarnings.
     * @param reason String object which is the description of the warning.
     * @param ex Throwable object which needs to be wrapped.
     */
    static SQLWarning createWarning(Throwable ex) {
    	String sourceName = null;
    	String modelName = null;
        if(ex instanceof SourceWarning) {
        	SourceWarning exception = (SourceWarning)ex;
        	if (exception.isPartialResultsError()) {
        		PartialResultsWarning warning = new PartialResultsWarning(JDBCPlugin.Util.getString("WarningUtil.Failures_occurred")); //$NON-NLS-1$
        		warning.addConnectorFailure(exception.getConnectorBindingName(), TeiidSQLException.create(exception));
        		return warning;
        	}
        	ex = exception.getCause();
        	sourceName = exception.getConnectorBindingName();
        	modelName = exception.getModelName();
        }
        String code = null;
        if (ex instanceof TeiidException) {
        	code = ((TeiidException)ex).getCode();
        }
        return new TeiidSQLWarning(ex.getMessage(), code, ex, sourceName, modelName);
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
