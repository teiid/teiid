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

package org.teiid.query.sql.util;

import org.teiid.core.TeiidComponentException;

/**
 * <p>Interface for interating through Expressions or values.  It may return
 * instances of Expression (which then have to be evaluated) or it may 
 * return some Object constant value - this should be checked for after 
 * calling {@link #next next}</p>  
 * 
 * <p>This interface is meant to abstract the details of how the values are 
 * stored and retrieved, if they are even stored in memory or not, etc. etc.
 * An implementation instance may or may not be resettable and therefore
 * reusable - see {@link #reset reset}.</p>
 */
public interface ValueIterator{
	
	/**
	 * Returns <tt>true</tt> if the iteration has more values. (In other
	 * words, returns <tt>true</tt> if <tt>next</tt> would return a value
	 * rather than throwing an exception.)
	 * @return <tt>true</tt> if this ValueIterator has more values.
	 * @throws TeiidComponentException indicating a non business-
	 * related Exception such as a service or bean being unavailable, or
	 * a communication failure.
	 */
	boolean hasNext()
	throws TeiidComponentException;
	
	/**
	 * Returns the next Expression or Object value in the interation.
	 * @return the next Expression or Object value in the iteration.
	 * @throws TeiidComponentException indicating a non business-
	 * related Exception such as a service or bean being unavailable, or
	 * a communication failure.
	 * @throws NoSuchElementException if iteration has no more elements.
	 */
	Object next()	
	throws TeiidComponentException;
	
	/**
	 * Optional reset method - allows a single instance of a
	 * ValueIterator implementation to be resettable, such that the
	 * next call to {@link #next next} returns the first element in
	 * the iteration (if any).  This method should be able to be
	 * called at any point during the lifecycle of a ValueIterator
	 * instance.
	 * @throws UnsupportedOperationException if this method is not 
	 * implemented
	 */
	void reset();
}
