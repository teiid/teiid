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

package com.metamatrix.common.buffer;

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;

/**
 * <p>A cursored source of tuples.  The implementation will likely be closely
 * bound to a {@link BufferManager} implementation - it will work with it
 * to use {@link TupleBatch TupleBatches} behind the scenes.</p>
 */
public interface TupleSource {

    /**
     * Returns the List of ElementSymbol describing the Tuple Source
     * @return the List of elements describing the Tuple Source
     */
	List<SingleElementSymbol> getSchema();
	
    /**
     * Returns the next tuple
     * @return the next tuple (a List object), or <code>null</code> if
     * there are no more tuples.
     * @throws MetaMatrixComponentException indicating a non-business
     * exception such as a communication exception, or other such
     * nondeterministic exception
     */
	List<?> nextTuple()
		throws MetaMatrixComponentException, MetaMatrixProcessingException;
	
    /**
     * Closes the Tuple Source.  
     * @throws MetaMatrixComponentException indicating a non-business
     * exception such as a communication exception, or other such
     * nondeterministic exception
     */    
	void closeSource()
		throws MetaMatrixComponentException;

}
