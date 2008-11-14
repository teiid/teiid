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

package com.metamatrix.admin.util;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

import com.metamatrix.admin.api.exception.AdminComponentException;
import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.exception.AdminProcessingException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.api.exception.MultipleRuntimeException;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.MetaMatrixRuntimeException;


/** 
 * Converts exceptions to the {@link AdminException} hierachy.
 * 
 * <p><b>NOTE</p>:  The conversion that takes place <b>does not</b>
 * replicate all exceptions in the chain.  The converison only replicates
 * the "root cause" or the exception that started the chain.</p>
 * 
 * <p>Exception logging should have been done with the original
 * exception before calling this conversion utility.  No logging
 * is done in this utility.</p>
 * 
 * @since 4.3
 */
public class AdminExceptionConverter {
    
    private final static String MSG_SEPARATOR = ": ";  //$NON-NLS-1$

    /** 
     * 
     * @since 4.3
     */
    public AdminExceptionConverter() {
        super();
    }
    
    /**
     * Take a <code>Throwable</code> and convert to an <code>AdminComponentException</code>
     * removing client dependancies on specific exception classes.
     * @param exceptionRoot the exception to convert.
     *  
     * @return the converted exception with the original error message and
     * stacktrace in a generic form.
     * @since 4.3
     */
    public static AdminComponentException convertToComponentException(Throwable exceptionRoot) {
        return convertToComponentException(exceptionRoot, null);
    }

    /**
     * Take a <code>Throwable</code> and convert to an <code>AdminProcessingException</code>
     * removing client dependancies on specific exception classes.
     * @param exceptionRoot the exception to convert.
     *  
     * @return the converted exception with the original error message and
     * stacktrace in a generic form.
     * @since 4.3
     */
    public static AdminProcessingException convertToProcessingException(Throwable exceptionRoot) {
        return convertToProcessingException(exceptionRoot, null);
    }

    /**
     * Take a <code>Throwable</code> and convert to an <code>AdminComponentException</code>
     * removing client dependancies on specific exception classes.
     * @param exceptionRoot The root exception to convert.
     * @param topLevelMsg optional top-level error message that, if present,
     * will be prepended to the error message of <code>e</code>.  Can be <code>null</code>.
     *  
     * @return the converted exception with the original error message and
     * stacktrace in a generic form.
     * @since 4.3
     */
    public static AdminComponentException convertToComponentException(Throwable exceptionRoot, String topLevelMsg) {
        AdminComponentException converted = null;
        
        // Get to the original error first.
        Throwable origError = getRootCause(exceptionRoot);
        
        if (origError instanceof AdminComponentException) {
            // Multiple exceptions have to be converted
            // during parsing, so we're done
            converted = (AdminComponentException)origError;
        } else {
            // Else convert orig to AdminException
            if (topLevelMsg == null) {
                converted = new AdminComponentException(origError.getClass().getName() + MSG_SEPARATOR + origError.getMessage());
            } else if (origError.getMessage() == null || topLevelMsg.equals(origError.getMessage())) {
                converted = new AdminComponentException(topLevelMsg);
            } else {
                converted = new AdminComponentException(topLevelMsg + MSG_SEPARATOR + origError.getMessage());
            }
            converted.setStackTrace(origError.getStackTrace());
        }
        return converted;
    }

    /**
     * Take a <code>Throwable</code> and convert to an <code>AdminProcessingException</code>
     * removing client dependancies on specific exception classes.
     * @param exceptionRoot The root exception to convert.
     * @param topLevelMsg optional top-level error message that, if present,
     * will be prepended to the error message of <code>e</code>.  Can be <code>null</code>.
     *  
     * @return the converted exception with the original error message and
     * stacktrace in a generic form.
     * @since 4.3
     */
    public static AdminProcessingException convertToProcessingException(Throwable exceptionRoot, String topLevelMsg) {
        AdminProcessingException converted = null;
        
        // Get to the original error first.
        Throwable origError = getRootCause(exceptionRoot);
        
        if (origError instanceof AdminProcessingException) {
            // Multiple exceptions have to be converted
            // during parsing, so we're done
            converted = (AdminProcessingException)origError;
        } else {
            // Now convert orig to AdminException
            if ( topLevelMsg == null ) {
                converted = new AdminProcessingException(origError.getClass().getName() + MSG_SEPARATOR + origError.getMessage());
            } else {
                converted = new AdminProcessingException(topLevelMsg + MSG_SEPARATOR + origError.getClass().getName() + 
                                                         MSG_SEPARATOR + origError.getMessage());
            }
            converted.setStackTrace(origError.getStackTrace());
            }
        return converted;
    }

    /**
     * Get the the bottom of the exception chain recursively.
     *  
     * <p>Note that "Multiple" MetaMatrix exceptions will always result in an 
     * AdminComponentException with multiple children.  The assumption is
     * that current MetaMatrix "Multiple" exceptions are "component" errors,
     * not user input errors.  There are no convertions
     * from "MultipleExceptions" to AdminProcessingExceptions.</p>
     * 
     * @param parent The MetaMatrix exception whose exception chain will be
     * walked
     * @return the root cause of the exception.
     * @since 4.3
     */
    private static Throwable getRootCause(final Throwable parent) {
        Throwable result = null;
        
        if (parent instanceof AdminException) {
            AdminException e = (AdminException)parent;
            result = getRootCause(e.getCause());
            if (result == null) {
                result = e;
            }
        } else if (parent instanceof MetaMatrixCoreException) {
            MetaMatrixCoreException e = (MetaMatrixCoreException) parent;
            result = getRootCause(e.getCause());
            if (result == null) {
                result = e;
            }
        } else if (parent instanceof MetaMatrixRuntimeException) {
            MetaMatrixRuntimeException e = (MetaMatrixRuntimeException) parent;
            result = getRootCause(e.getChild());
            if (result == null) {
                result = e;
            }
        } else if (parent instanceof InvocationTargetException) {
            InvocationTargetException e = (InvocationTargetException) parent;
            result = getRootCause(e.getCause());
            if (result == null) {
                result = e;
            }
        } else if (parent instanceof MultipleException) {
            MultipleException e = (MultipleException) parent;
            
            // Create converted parent
            AdminComponentException newRoot = new AdminComponentException(e.getMessage());
            newRoot.setStackTrace(e.getStackTrace());
            
            // Add all child leaves
            for (Iterator iter = e.getExceptions().iterator(); iter.hasNext();) {
                result = getRootCause((Throwable)iter.next());
                if (result == null) {
                    AdminException aResult = convertBasic(e);
                    newRoot.addChild(aResult);
                }
            }
            result = newRoot;
        } else if (parent instanceof MultipleRuntimeException) {
            MultipleRuntimeException e = (MultipleRuntimeException) parent;
            
            // Create converted parent
            AdminComponentException newRoot = new AdminComponentException(e.getMessage());
            newRoot.setStackTrace(e.getStackTrace());
            
            // Add all child leaves
            for (Iterator iter = e.getThrowables().iterator(); iter.hasNext();) {
                result = getRootCause((Throwable)iter.next());
                if (result == null) {
                    AdminException aResult = convertBasic(e);
                    newRoot.addChild(aResult);
                }
            }
            result = newRoot;
        } else {
            // Safe to set result to the Throwable parent
            // since we've taken care of special exceptions above
            result = parent;
        }
        
        return result;
    }

    /** 
     * Convert a Throwable to a final AdminException type.
     * @param toConvert The exception to convert.
     * @return the converted exception being one of AdminCompontentException or
     * AdminProcessingException
     * @since 4.3
     */
    private static AdminException convertBasic(Throwable toConvert) {
        AdminException converted = null;
        
        if ( toConvert instanceof MetaMatrixProcessingException ) {
            converted = new AdminProcessingException(toConvert.getMessage());
            converted.setStackTrace(toConvert.getStackTrace());
        } else {
            // Default to AdminComponentException
            converted = new AdminComponentException(toConvert.getMessage());
            converted.setStackTrace(toConvert.getStackTrace());
        }
        
        return converted;
    }

}
