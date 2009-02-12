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

import java.util.*;

/**
 * Exception that represents the occurrence of multiple exceptions.
 */
public class MultipleException extends Exception {
    // =========================================================================
    //                       C O N S T R U C T O R S
    // =========================================================================

//    private int attribute1;
    /**
     *The set of Throwable instances that make up this exception
     * @link aggregation
     * @associates <b>java.lang.Throwable</b>
     * @supplierCardinality 1..*
     */
    private List throwablesList = new ArrayList();

    /** An error code. */
    private String code;
    /** Construct a default instance of this class. */
    public MultipleException() {
        super();
    }

    /**
     * Construct an instance with the error message specified.
     *
     * @param message The error message
     */
    public MultipleException( String message ) {
        super( message );
    }

    /**
     * Construct an instance with an error code and message specified.
     *
     * @param message The error message
     * @param code    The error code
     */
    public MultipleException( String code, String message ) {
        super( message );
        setCode( code );
    }

    /**
     * Construct an instance with the set of exceptions specified.
     *
     * @param throwables the set of exceptions that is to comprise
     * this exception
     */
    public MultipleException( Collection throwables ) {
        super();
        setExceptions(throwables);
    }

    /**
     * Construct an instance with the set of exceptions and error message
     * specified.
     *
     * @param throwables the set of exceptions that is to comprise
     * this exception
     * @param message The error message
     */
    public MultipleException( Collection throwables, String message ) {
        super( message );
        setExceptions(throwables);
    }

    /**
     * Construct an instance with the set of exceptions, and an error code and
     * message specified.
     *
     * @param throwables the set of exceptions that is to comprise
     * this exception
     * @param message The error message
     * @param code    The error code
     */
    public MultipleException( Collection throwables, String code, String message ) {
        super( message );
        setExceptions(throwables);
        setCode( code );
    }

    /**
     * Get the code for this exception.
     * @return the code value
     */
    public String getCode(){
            return code;
	}
	
    /**
     * Set the code for this exception.
     * @param code the new code value
     */
    public void setCode(String code){
            this.code = code;
        }

    /**
     * Obtain the set of exceptions that comprise this exception.
     * @return the set of Throwable instances that comprise this exception.
     */
    public List getExceptions() {
    	return this.throwablesList;
    }

    /**
     * Set the exceptions that comprise this exception.
     * @param throwables the set of exceptions that is to comprise
     * this exception
     */
    public void setExceptions( Collection throwables ){
        this.throwablesList = new ArrayList(throwables);
    }
}

