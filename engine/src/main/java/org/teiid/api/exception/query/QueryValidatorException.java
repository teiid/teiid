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

package org.teiid.api.exception.query;

import org.teiid.core.BundleUtil;

/**
 * This exception is thrown if an error is discovered while validating the query.  Validation
 * checks a number of aspects of a query to ensure that the query is semantically valid.
 */
public class QueryValidatorException extends QueryProcessingException {

	private static final long serialVersionUID = 7003393883967513820L;

	/**
     * No-arg constructor required by Externalizable semantics.
     */
    public QueryValidatorException() {
        super();
    }
    
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public QueryValidatorException( String message ) {
        super( message );
    }

    public QueryValidatorException(Throwable e) {
        super(e);
    }
    
    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public QueryValidatorException(Throwable e, String message ) {
        super( e, message );
    }
    
    public QueryValidatorException(BundleUtil.Event event, Throwable e) {
        super( event, e);
    }    
    
    public QueryValidatorException(BundleUtil.Event event, Throwable e, String msg) {
        super(event, e, msg);
    }
    
    public QueryValidatorException(BundleUtil.Event event, String msg) {
        super(event, msg);
    }    
}
