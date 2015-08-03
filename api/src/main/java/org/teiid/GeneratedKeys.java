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

package org.teiid;

import java.util.Iterator;
import java.util.List;

public interface GeneratedKeys {
	
	/**
	 * Add a generated key to this result.  The list values must match the class types of this result.
	 * @param vals
	 */
	void addKey(List<?> vals);
	
	/**
	 * Get the column names of this result.
	 * @return
	 */
	String[] getColumnNames();
	
	/**
	 * Get the column types of this result.
	 * @return
	 */
	Class<?>[] getColumnTypes();
	
	/**
	 * Get an iterator to the keys added to this result.  The iterator is not guaranteed to be thread-safe
	 * with respect to the {@link #addKey(List)} method.
	 * @return
	 */
	Iterator<List<?>> getKeyIterator();
	
}
