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

import java.util.Set;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.sql.symbol.Expression;



/** 
 * The ValueIteratorSource lets a language object that needs a ValueIterator hold this 
 * reference to the source of the ValueIterator as a reference until the ValueIterator 
 * can be ready.  
 *  
 * @since 5.0.1
 */
public interface ValueIteratorSource {
    
    /**
     * Attempt to obtain a ValueIterator from this source.  If the iterator is 
     * not ready yet, return null to indicate that.
     * @param valueExpression The expression we are retrieving an iterator for  
     * @return ValueIterator if ready, null otherwise
     * @throws TeiidComponentException 
     * @since 5.0.1
     */
    ValueIterator getValueIterator(Expression valueExpression) throws TeiidComponentException;
    
    Set<Object> getCachedSet(Expression valueExpression) throws TeiidComponentException, TeiidProcessingException;

	void setUnused(boolean unused);

	boolean isUnused();

}
