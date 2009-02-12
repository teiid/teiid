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

package com.metamatrix.query.sql.util;

/**
 * <p>Language Object Implementors of this interface will provide a 
 * {@link ValueIterator} over one or potentially more values
 * that they have.</p>  
 * 
 * <p>Also, in some cases implementors may need to
 * have their value iterators given to them by some external
 * entity, so there is a setter method in this interface.</p>  
 */
public interface ValueIteratorProvider {

    /**
     * Get the {@link ValueIterator} from this ValueIterator provider
     * @return ValueIterator over the values of this instance
     */
    ValueIterator getValueIterator();

    /**
     * Set the {@link ValueIterator} instance on this ValueIterator
     * provider.  Note that the implementor may choose to not
     * implement this method, and the method call would have no 
     * effect.
     * @param valueIterator an instance of ValueIterator to be set
     * on this ValueIterator provider (as in the case of subquery
     * containers whose results are provided externally).
     */
    void setValueIterator(ValueIterator valueIterator);

}
