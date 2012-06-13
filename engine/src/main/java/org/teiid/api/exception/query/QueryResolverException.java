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

import java.util.*;

import org.teiid.core.BundleUtil;

/**
 * This exception represents the case where the query submitted could not resolved
 * when it is checked against the metadata
 */
public class QueryResolverException extends QueryProcessingException {

	private static final long serialVersionUID = 752912934870580744L;
	private transient List problems;

    /**
     * No-arg constructor required by Externalizable semantics.
     */
    public QueryResolverException() {
        super();
    }
    
    /**
     * Construct an instance with the message specified.
     *
     * @param message A message describing the exception
     */
    public QueryResolverException( String message ) {
        super( message );
    }

    public QueryResolverException(Throwable e) {
        super(e);
    }
    
    /**
     * Construct an instance from a message and an exception to chain to this one.
     *
     * @param message A message describing the exception
     * @param e An exception to nest within this one
     */
    public QueryResolverException( Throwable e, String message ) {
        super( e, message );
    }

    public QueryResolverException(BundleUtil.Event event, Throwable e) {
        super( event, e);
    }    
    
    public QueryResolverException(BundleUtil.Event event, Throwable e, String msg) {
        super(event, e, msg);
    }
    
    public QueryResolverException(BundleUtil.Event event, String msg) {
        super(event, msg);
    }	
    
	/**
	 * Set the list of unresolved symbols during QueryResolution
	 * @param unresolvedSymbols List of <UnresolvedSymbolDescription> objects
	 */
	public void setUnresolvedSymbols(List unresolvedSymbols) {
		this.problems = unresolvedSymbols;
	}

    /**
     * Add an UnresolvedSymbolDescription to the list of unresolved symbols
     * @param symbolDesc Single description 
     */
    public void addUnresolvedSymbol(UnresolvedSymbolDescription symbolDesc) { 
        if(this.problems == null) { 
            this.problems = new ArrayList();
        }
        this.problems.add(symbolDesc);
    }
        
	/**
	 * Set the list of unresolved symbols during QueryResolution
	 * @return List of {@link UnresolvedSymbolDescription} objects
	 */
	public List getUnresolvedSymbols() {
		return this.problems;
	}
}
