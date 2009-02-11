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

package com.metamatrix.api.exception.query;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Thrown when a query cannot be parsed.  This is most likely due to not 
 * following the Query Parser grammar, which defines how queries are parsed.
 */
public class QueryParserException extends QueryProcessingException {

	// Error location, if known
	private int line = -1;
	private int column = -1;

    /**
     * No-arg constructor required by Externalizable semantics.
     */
    public QueryParserException() {
        super();
    }
    
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public QueryParserException( String message ) {
        super( message );
    }

    /**
     * Construct an instance with the message and error code specified.
     *
     * @param message A message describing the exception
     * @param code The error code
     */
    public QueryParserException( String code, String message ) {
        super( code, message );
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public QueryParserException( Throwable e, String message ) {
        super( e, message );
    }

    /**
     * Construct an instance from a message and a code and an exception to
     * chain to this one.
     *
     * @param e An exception to nest within this one
     * @param message A message describing the exception
     * @param code A code denoting the exception
     */
    public QueryParserException( Throwable e, String code, String message ) {
        super( e, code, message );
    }
	
	/**
	 * Set location of error
	 * @param line Line error occurred on in input
	 * @param column Column error occurred on in input
	 */
	public void setErrorLocation(int line, int column) {
		this.line = line;
		this.column = column;
	}
	
	/**
	 * Determine if location of error in string being parsed is known.  If 
	 * so, the line and column can be obtained with getLine() and getColumn().
	 * @return True if location is known
	 */
	public boolean isLocationKnown() {
		return this.line > -1;
	}	
	
	/** 
	 * Get line error occurred on in string being parsed.
	 * @return Line error occurred on in input string
	 */
	public int getLine() {
		return this.line;
	}
	
	/**
	 * Get column error occurred on in string being parsed.
	 * @return Column error occurred on in input string
	 */
	public int getColumn() {
		return this.column;
	}		
	 	
    /**
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        line = in.readInt();
        column = in.readInt();
    }

    /**
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(line);
        out.writeInt(column);
    }

}
