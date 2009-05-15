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

package com.metamatrix.core;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.SQLException;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.core.util.MetaMatrixExceptionUtil;

/**
 * A generic runtime exception which contains a reference to another exception
 * and which represents a condition that should never occur during runtime.  This
 * class can be used to maintain a linked list of exceptions. <p>
 *
 * Subclasses of this exception typically only need to implement whatever
 * constructors they need. <p>
 */
public class MetaMatrixRuntimeException extends RuntimeException {
    public static final long serialVersionUID = -4035276728007979320L;
    
    private static final String EMPTY_STRING = ""; //$NON-NLS-1$
    public static final String CAUSED_BY_STRING = CorePlugin.Util.getString("MetaMatrixRuntimeException.Caused_by"); //$NON-NLS-1$
    
    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################
    
    /**
     * Utility method to get the name of a class, without package information.
     *
     * @param cls The class to get the name of
     * @return The name of the class, without package info
     */
    public static String getClassShortName( Class cls ) {
        if ( cls == null ) return EMPTY_STRING;
        String className = cls.getName();
        return className.substring( className.lastIndexOf('.')+1 );
    }
    
    //############################################################################################################################
    //# Variables                                                                                                                #
    //############################################################################################################################

    /** An error code. */
    private String code;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    /**
     * Construct a default instance of this class.
     */
    public MetaMatrixRuntimeException() {
    }

    /**
     * Construct an instance with the specified error message.  If the message is actually a key, the actual message will be
     * retrieved from a resource bundle using the key, the specified parameters will be substituted for placeholders within the
     * message, and the code will be set to the key.
     * @param message The error message or a resource bundle key
     */
    public MetaMatrixRuntimeException(final String message) {
        super(message);
    }

    /**
     * Construct an instance with the specified error code and message.  If the message is actually a key, the actual message will
     * be retrieved from a resource bundle using the key, and the specified parameters will be substituted for placeholders within
     * the message.
     * @param code    The error code 
     * @param message The error message or a resource bundle key
     */
    public MetaMatrixRuntimeException(final int code, final String message) {
        super(message);
        // The following setCode call should be executed after setting the message 
        setCode(code);
    }
    
    public MetaMatrixRuntimeException(final String code, final String message) {
        super(message);
        // The following setCode call should be executed after setting the message 
        setCode(code);
    }

    /**
     * Construct an instance with a linked exception specified.  If the exception is a {@link MetaMatrixCoreException} or a
     * MetaMatrixRuntimeException, then the code will be set to the exception's code.
     * @param e An exception to chain to this exception
     */
    public MetaMatrixRuntimeException(final Throwable e) {
        this(e, ( e instanceof java.lang.reflect.InvocationTargetException )
                   ? ((java.lang.reflect.InvocationTargetException)e).getTargetException().getMessage()
                   : (e == null ? null : e.getMessage()));        
    }

    /**
     * Construct an instance with the linked exception and error message specified.  If the message is actually a key, the error
     * message will be retrieved from a resource bundle the key, and code will be set to that key.  Otherwise, if the specified
     * exception is a {@link MetaMatrixCoreException} or a MetaMatrixRuntimeException, the code will be set to the exception's code.
     * @param e       The exception to chain to this exception
     * @param message The error message or a resource bundle key
     */
    public MetaMatrixRuntimeException(final Throwable e, final String message) {
        super(message, e);
        setCode(e);
    }

    /**
     * Construct an instance with the linked exception, error code, and error message specified.  If the message is actually a
     * key, the error message will be retrieved from a resource bundle using the key.
     * @param e       The exception to chain to this exception
     * @param code    The error code 
     * @param message The error message or a resource bundle key
     */
    public MetaMatrixRuntimeException(final Throwable e, final int code, final String message) {
        super(message, e);
        // The following setCode call should be executed after setting the message 
        setCode(code);
    }
    
    /**
     * Construct an instance with the linked exception, error code, and error message specified. If the specified
     * exception is a {@link MetaMatrixException} or a MetaMatrixRuntimeException, the code will
     * be set to the exception's code.
     * @param e       The exception to chain to this exception
     * @param code    The error code
     * @param message The error message
     */
    public MetaMatrixRuntimeException(final Throwable e, final String code, final String message) {
        super(message, e);
        // Overwrite code set in other ctor from exception.
        setCode(code);
    }


    //############################################################################################################################
    //# Methods                                                                                                                  #
    //############################################################################################################################

    /**
     * Get the exception which is linked to this exception.
     *
     * @return The linked exception
     */
    public Throwable getChild() {
        return this.getCause();
    }
    
    /**
     * Get the error code.
     *
     * @return The error code 
     */
    public String getCode() {
    	if (code == null) {
    		return "0"; //$NON-NLS-1$
    	}
        return this.code;
    }
    
    public int getIntCode() {
    	if (code == null) {
    		return 0;
    	}
        try {
        	return Integer.parseInt(code);
        } catch (NumberFormatException e) {
        	
        }
        return 0;
    }

    /**
     * Set the error code.
     *
     * @param code The error code 
     */
    private void setCode( int code ) {
        this.code = Integer.toString(code);
    }
    
    private void setCode( String code ) {
        this.code = code;
    }

    private void setCode(Throwable e) {
        if (e instanceof MetaMatrixCoreException) {
            this.code = (((MetaMatrixCoreException) e).getCode());
        } else if (e instanceof MetaMatrixRuntimeException) {
        	this.code = ((MetaMatrixRuntimeException) e).getCode();
        } else if (e instanceof SQLException) {
        	this.code = Integer.toString(((SQLException)e).getErrorCode());
        }
    }

    /**
     * Returns a string representation of this class.
     *
     * @return String representation of instance
     */
    public String toString() {
        return MetaMatrixExceptionUtil.getLinkedMessages(this);
    }

    /* 
     * @see com.metamatrix.core.util.MetaMatrixNestedException#superPrintStackTrace(java.io.PrintStream)
     */
    public void superPrintStackTrace(PrintStream output) {
        super.printStackTrace(output);
    }

    /* 
     * @see com.metamatrix.core.util.MetaMatrixNestedException#superPrintStackTrace(java.io.PrintWriter)
     */
    public void superPrintStackTrace(PrintWriter output) {
        super.printStackTrace(output);
    }
    
}
