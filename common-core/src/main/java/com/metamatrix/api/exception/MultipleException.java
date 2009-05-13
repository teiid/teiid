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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

/**
 * Exception that represents the occurrence of multiple exceptions.
 */
public class MultipleException extends Exception implements Externalizable {

	/**
     *The set of Throwable instances that make up this exception
     * @link aggregation
     * @associates <b>java.lang.Throwable</b>
     * @supplierCardinality 1..*
     */
    private List throwablesList = null;

    /** An error code. */
    private String code;
    
    
    /**
     * No-arg Constructor
     */
    public MultipleException() {
    	super();
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
        this( throwables, null, message );
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
    public MultipleException( Collection<Throwable> throwables, String code, String message ) {
        super( message );
        this.throwablesList = Collections.unmodifiableList(new ArrayList<Throwable>(throwables));
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

	//## JDBC4.0-begin ##
	@Override
	//## JDBC4.0-end ##
	public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
		this.code = (String)in.readObject();
		this.throwablesList = ExceptionHolder.toThrowables((List<ExceptionHolder>)in.readObject());
	}

	//## JDBC4.0-begin ##
	@Override
	//## JDBC4.0-end ##
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(code);
		out.writeObject(ExceptionHolder.toExceptionHolders(throwablesList));
	}
}

