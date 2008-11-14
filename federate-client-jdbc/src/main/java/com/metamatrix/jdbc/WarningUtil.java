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

package com.metamatrix.jdbc;

import java.sql.SQLWarning;
import java.util.*;
import java.util.Collection;
import java.util.Iterator;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.dqp.exception.PartialResultsException;
import com.metamatrix.dqp.exception.SourceFailureDetails;
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
        if(ex instanceof PartialResultsException) {
            return convertToWarning((PartialResultsException)ex);
        }
        return new SQLWarning(ex.getMessage());
    }

    /**
     * Get child exception based on type of parent
     * @param parent Exception parent
     * @return Child if there is one or null otherwise
     */
    static Throwable getChildException(Throwable parent) {
        Throwable exception;
        if (parent instanceof MetaMatrixException) {
            exception = ((MetaMatrixException) parent).getChild();
        } else if (parent instanceof SQLWarning) {
            exception = ((SQLWarning) parent).getNextException();
        } else {
            exception = null;   
        }
        return exception;
    }

    /**
     * Used to convert a PartialResultsException into a PartialResultsWarning
     * @param exception The source exception sent from the server
     * @return The warning that is being sent to the user
     */    
    static PartialResultsWarning convertToWarning(PartialResultsException exception) {
        PartialResultsWarning warning = new PartialResultsWarning(JDBCPlugin.Util.getString("WarningUtil.Failures_occurred")); //$NON-NLS-1$
        
        Collection failures = exception.getSourceFailureDetails();
        for(Iterator iter = failures.iterator(); iter.hasNext(); ) {
            SourceFailureDetails details = (SourceFailureDetails) iter.next();
            String connectorName = details.getConnectorBindingName();
            MetaMatrixException sourceEx = details.getException();

            warning.addConnectorFailure(connectorName, MMSQLException.create(sourceEx));                                  
        }
        
        return warning;
    }

    /**
     * Convert a List of warnings from the server into a single SQLWarning chain.
     * @param exceptions List of exceptions from server
     * @return Chain of SQLWarning corresponding to list of exceptions
     */
    static SQLWarning convertWarnings(List exceptions) {
        if(exceptions == null || exceptions.size() == 0) {
            return null;    
        } else if(exceptions.size() == 1) {
            return createWarning((Exception) exceptions.get(0));   
        } else {
            Iterator exIter = exceptions.iterator();
            SQLWarning warning = null;

            Throwable childException = (Throwable) exIter.next();  // child exception or warning       
            while(childException != null) {
                SQLWarning newWarning = createWarning(childException); 
                if(warning == null) {
                    warning = newWarning;
                } else {
                    // This call can be made multiple times - it adds the next exception to 
                    // the END of the chain (which is quite tricky)
                    warning.setNextWarning(newWarning);
                }
    
                // Walk chain or switch to next exception in iterator                
                childException = getChildException(childException);
                if(childException == null && exIter.hasNext()) {
                    childException = (Throwable) exIter.next();
                }
            }
         
            return warning;   
        }
    }


}
