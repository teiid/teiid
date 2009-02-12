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

package com.metamatrix.query.processor;

import java.util.List;

import com.metamatrix.common.buffer.TupleSource;

/**
 * <p>This class can be used to represent null tuples or no data for an
 * atomic query execution. Example usage is in implementation of partial
 * results feature where we have to return null tuples if there is an error
 * in the execution of atomic query.</p>
 */

public class NullTupleSource implements TupleSource {

	private List elements; // elementSymbols containing null tuples
	
	/**
	 * <p>Constructor that takes the list of elementSymbols
	 * that have null rows.</p>
	 * @param elements List of elementSymbols that this source contains.
	 */	
	public NullTupleSource(List elements) {
		this.elements = elements;
	}
	
    /**
     * @see TupleSource#getSchema()
     */
    public List getSchema() {
        return this.elements;
    }
    
	/**
	 * <p>Open the tuple source to read tuples. This method does nothing.</p>
	 */
	public void openSource() {
		// do nothing
	}

	/**
	 * <p>This method returns a null indicating, that there are no
	 * rows in the source.</p>
	 * @return list containing the values of the next tuple.
	 */
	public List nextTuple() {
		return null;
	}
	
	/**
	 * <p>Close the tuple source after reading tuples. This method does nothing.</p>
	 */	
	public void closeSource() {
		// do nothing
	}

} // END CLASS
