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

package com.metamatrix.api.exception;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * MultipleRuntimeException is intended for situations where multiple, unrelated Throwables need to be thrown as a single
 * RuntimeException.  This is useful, for example, when an Exception is thrown in both the try and finally portions of a try-
 * finally block.  The Throwables are maintained within a list to allow them to be ordered by level of importance, with the first
 * Throwable having the highest importance.
 * @since 2.1
 */
public class MultipleRuntimeException extends RuntimeException
implements Serializable {
    //############################################################################################################################
    //# Variables                                                                                                                #
    //############################################################################################################################

    private List throwables;
    private String code;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    /**
     * Constructs an instance of this class with no message, a zero error code, and no Throwables.
     * @since 2.1
     */
    public MultipleRuntimeException() {
        this(null, null, (List)null);
    }

    /**
    Constructs an instance of this class with the specified message, a zero error code, and no Throwables.
    @param message The message describing this Exception
    @since 2.1
    */
    public MultipleRuntimeException(final String message) {
        this(message, null, (List)null);
    }

    /**
    Constructs an instance of this class with no message, a zero error code, and the specified set of Throwables.
    @param throwables The list of Throwables that comprise this Exception
    @since 2.1
    */
    public MultipleRuntimeException(final List throwables) {
        this(null, null, throwables);
    }

    /**
    Constructs an instance of this class with no message, a zero error code, and the specified set of Throwables.
    @param throwables The list of Throwables that comprise this Exception
    @since 2.1
    */
    public MultipleRuntimeException(final Throwable[] throwables) {
        this(null, null, Arrays.asList(throwables));
    }

    /**
    Constructs an instance of this class with the specified message, the specified error code, and no Throwables.
    @param message		The message describing this Exception
    @param code			The error code associated with this Exception
    @since 2.1
    */
    public MultipleRuntimeException(final String message, final String code) {
        this(message, code, (List)null);
    }

    /**
    Constructs an instance of this class with the specified message, a zero error code, and the specified set of Throwables.
    @param message		The message describing this Exception
    @param throwables	The list of Throwables that comprise this Exception
    @since 2.1
    */
    public MultipleRuntimeException(final String message, final List throwables) {
        this(message, null, throwables);
    }

    /**
    Constructs an instance of this class with the specified message, a zero error code, and the specified set of Throwables.
    @param message		The message describing this Exception
    @param throwables	The list of Throwables that comprise this Exception
    @since 2.1
    */
    public MultipleRuntimeException(final String message, final Throwable[] throwables) {
        this(message, null, Arrays.asList(throwables));
    }

    /**
    Constructs an instance of this class with the specified message, the specified error code, and the specified set of
    Throwables.
    @param message		The message describing this Exception
    @param code			The error code associated with this Exception
    @param throwables	The list of Throwables that comprise this Exception
    @since 2.1
    */
    public MultipleRuntimeException(final String message, final String code, final Throwable[] throwables) {
        this(message, code, Arrays.asList(throwables));
    }

    /**
    Constructs an instance of this class with the specified message, the specified error code, and the specified set of
    Throwables.
    @param message		The message describing this Exception
    @param code			The error code associated with this Exception
    @param throwables	The list of Throwables that comprise this Exception
    @since 2.1
    */
    public MultipleRuntimeException(final String message, final String code, final List throwables) {
        super(message);
        constructMultipleRuntimeException(code, throwables);
    }

    //############################################################################################################################
    //# Methods                                                                                                                  #
    //############################################################################################################################

    /**
    Adds the specified Throwable to the list of Throwables that comprise this Exception.
    @param throwable The Throwable
    @since 2.1
    */
    public void addThrowable(final Throwable throwable) {
        addThrowable(throwable, throwables.size());
    }

    /**
    Adds the specified Throwable to the list of Throwables that comprise this Exception at the specified index.
    @param throwable	The Throwable
    @param index		The index within the list of Throwables at which the Throwable should be added
    @since 2.1
    */
    public void addThrowable(final Throwable throwable, final int index) {
        if (throwable == null) {
            throw new IllegalArgumentException("throwable param cannot be null"); //$NON-NLS-1$
        }
        if (index < 0) {
            throw new IllegalArgumentException("index cannot be negative"); //$NON-NLS-1$
        }
       	throwables.add(index, throwable);
    }

    /**
    Constructs an instance of this class with the specified error code and the specified set of Throwables.
    @param code			The error code associated with this Exception
    @param throwables	The list of Throwables that comprise this Exception
    @since 2.1
    */
    protected void constructMultipleRuntimeException(final String code, final List throwables) {
        setCode(code);
        setThrowables(throwables);
    }

    /**
    Returns the error code associated with this Exception.
    @return The error code
    @since 2.1
    */
    public String getCode() {
        return code;
    }

    /**
    Returns the list of Throwables that comprise this Exception (Guaranteed not to be null).
    @return The list of Throwables
    @since 2.1
    */
    public List getThrowables() {
        return throwables;
    }

    /**
    Sets the error code associated with this Exception.
    @param code The error code associated with this Exception
    @since 2.1
    */
    public void setCode(final String code) {
        this.code = code;
    }

    /**
    Sets the list of Throwables that comprise this Exception.
    @param throwables The list of Throwables that comprise this Exception
    @since 2.1
    */
    public void setThrowables(final List throwables) {
        // Could iterate through the list to assert each entry is a Throwable...?
        if (throwables == null) {
            this.throwables = new ArrayList();
        } else {
        	this.throwables = throwables;
        }
    }

    /**
    Overridden to return the concatentation of the toString() results from each of the Throwables within this Exception.
    @since 2.1
    @return the string representation
    */
    public String toString() {
        final StringBuffer buf = new StringBuffer();
        final Iterator iter = throwables.iterator();
        while (iter.hasNext()) {
            buf.append(iter.next().toString());
        }
        return buf.toString();
    }
}
