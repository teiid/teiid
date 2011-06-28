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

package org.teiid.core;

import org.teiid.core.util.ExceptionUtil;


/**
 * A generic runtime exception which contains a reference to another exception
 * and which represents a condition that should never occur during runtime.  This
 * class can be used to maintain a linked list of exceptions. <p>
 *
 * Subclasses of this exception typically only need to implement whatever
 * constructors they need. <p>
 */
public class TeiidRuntimeException extends RuntimeException {
    public static final long serialVersionUID = -4035276728007979320L;
    
    public static final String CAUSED_BY_STRING = CorePlugin.Util.getString("RuntimeException.Caused_by"); //$NON-NLS-1$
    
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
    public TeiidRuntimeException() {
    }

    /**
     * Construct an instance with the specified error message.  If the message is actually a key, the actual message will be
     * retrieved from a resource bundle using the key, the specified parameters will be substituted for placeholders within the
     * message, and the code will be set to the key.
     * @param message The error message or a resource bundle key
     */
    public TeiidRuntimeException(final String message) {
        super(message);
    }

    public TeiidRuntimeException(final String code, final String message) {
        super(message);
        // The following setCode call should be executed after setting the message 
        setCode(code);
    }

    /**
     * Construct an instance with a linked exception specified.  If the exception is a {@link TeiidException} or a
     * MetaMatrixRuntimeException, then the code will be set to the exception's code.
     * @param e An exception to chain to this exception
     */
    public TeiidRuntimeException(final Throwable e) {
        this(e, ( e instanceof java.lang.reflect.InvocationTargetException )
                   ? ((java.lang.reflect.InvocationTargetException)e).getTargetException().getMessage()
                   : (e == null ? null : e.getMessage()));        
    }

    /**
     * Construct an instance with the linked exception and error message specified.  If the message is actually a key, the error
     * message will be retrieved from a resource bundle the key, and code will be set to that key.  Otherwise, if the specified
     * exception is a {@link TeiidException} or a MetaMatrixRuntimeException, the code will be set to the exception's code.
     * @param e       The exception to chain to this exception
     * @param message The error message or a resource bundle key
     */
    public TeiidRuntimeException(final Throwable e, final String message) {
        super(message, e);
        setCode(TeiidException.getCode(e));
    }

    /**
     * Construct an instance with the linked exception, error code, and error message specified. If the specified
     * exception is a {@link TeiidException} or a MetaMatrixRuntimeException, the code will
     * be set to the exception's code.
     * @param e       The exception to chain to this exception
     * @param code    The error code
     * @param message The error message
     */
    public TeiidRuntimeException(final Throwable e, final String code, final String message) {
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
     * @deprecated use {@link #getCause()} instead
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
        return this.code;
    }
    
    private void setCode( String code ) {
        this.code = code;
    }

    /**
     * Returns a string representation of this class.
     *
     * @return String representation of instance
     */
    public String toString() {
        return ExceptionUtil.getLinkedMessages(this);
    }

}
