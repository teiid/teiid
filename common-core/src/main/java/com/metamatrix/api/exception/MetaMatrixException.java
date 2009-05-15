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

package com.metamatrix.api.exception;

import java.util.Iterator;

import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.MetaMatrixExceptionUtil;

/**
 * <p>A generic exception which contains a reference to another exception.  This
 * class can be used to maintain a linked list of exceptions. </p>
 *
 * <p>Subclasses of this exception typically only need to implement whatever
 * constructors they need. <p>
 *
 * <p><b>This class assumes all messages internationalization has been resolved
 * <i>before</i> the constructor is called.  There will be no automatic lookup
 * performed by this class.</b></p>
 */
public class MetaMatrixException extends MetaMatrixCoreException {

    /**
     * No-arg Constructor
     */
    public MetaMatrixException() {
        super();
    }

    /**
     * Construct an instance with the specified error message.
     * @param message The error message
     */
    public MetaMatrixException(final String message) {
        super(message);
    }

    /**
     * Construct an instance with the specified error code and message.
     * @param code    The error code
     * @param message The error message
     */
    public MetaMatrixException(final String code, final String message) {
        super(code, message);
    }

    /**
     * Construct an instance with a linked exception specified.  If the exception is a MetaMatrixException or a
     * {@link MetaMatrixRuntimeException}, then the code will be set to the exception's code.
     * @param e An exception to chain to this exception
     */
    public MetaMatrixException(final Throwable e) {
        this(e, e==null ? null : e.getMessage());
    }

    /**
     * Construct an instance with the linked exception and error message specified. If the exception is a
     * MetaMatrixException or a {@link MetaMatrixRuntimeException}, then the code will be set to the exception's
     * code.
     * @param e       The exception to chain to this exception
     * @param message The error message
     */
    public MetaMatrixException(final Throwable e, final String message) {
        super(e, message);
    }

    /**
     * Construct an instance with the linked exception, error code, and error message specified. If the exception is a
     * MetaMatrixException or a {@link MetaMatrixRuntimeException}, then the code will be set to the exception's
     * code.
     * @param e       The exception to chain to this exception
     * @param code    The error code
     * @param message The error message
     */
    public MetaMatrixException(final Throwable e, final String code, final String message) {
        super(e, code, message);
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
        return super.getCause();
    }

    /**
     * Returns the error message, formatted for output. <P>
     *
     * The default formatting provided by this method is to prepend the
     * error message with the level and the name of the class, and to
     * append the error code on the end if a non-zero code is defined. <P>
     *
     * This method provides a hook for subclasses to override the default
     * formatting of any one exception.
     *
     * @param throwable The exception to print
     * @param level The depth of the exception in the chain of exceptions
     * @return A formatted string for the exception
     */
    static String getFormattedMessage(final Throwable throwable, final int level) {
        String code;
        if (throwable instanceof MetaMatrixException) {
            code = ((MetaMatrixException) throwable).getCode();
        } else if (throwable instanceof MetaMatrixRuntimeException) {
            code = ((MetaMatrixRuntimeException) throwable).getCode();
        } else {
            code = null;
        }
        return ((level != 0) ? ("\n" + level + " ") : "") //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                + "[" + throwable.getClass().getSimpleName() + "]" //$NON-NLS-1$ //$NON-NLS-2$
                + ((code != null) ? (' ' + code + ": ") : "") //$NON-NLS-1$ //$NON-NLS-2$
                + (throwable.getMessage() == null ? "" : throwable.getMessage()); //$NON-NLS-1$
    }

    /**
     * Get the full error message, including any message(s) from child
     * exceptions.  Messages of any exceptions chained to this exception are
     * prepended with their "level" in the chain.
     *
     * @return The full error message
     *
     * @see #getFormattedMessage
     */
    public String getFullMessage() {
    	int level = 0;
        StringBuffer buf = new StringBuffer();
        buf.append(getFormattedMessage(this,level));
        Iterator children = MetaMatrixExceptionUtil.getChildrenIterator(this);
        while ( children.hasNext() ){
            level++;
            Throwable exception = (Throwable)children.next();
            buf.append(getFormattedMessage(exception,level));
        }
        return buf.toString();
    }

    /**
     * Returns a string representation of this class.
     *
     * @return String representation of instance
     */
    public String toString() {
        return getFullMessage();
    }

} // END CLASS


