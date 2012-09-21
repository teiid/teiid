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
package org.teiid.translator.object;

import java.util.List;

import org.teiid.language.Select;
import org.teiid.translator.TranslatorException;

/**
 * Each ObjectConnection implementation represents a connection instance and is responsible for
 * implementing the data source search language specifics for searching its cache.
 * 
 * @author vhalbert
 *
 */
public interface ObjectConnection  {
	
	/**
	 * Call to perform the search on the cache identified by this connection instance
	 * @param command
	 * @return List of Objects
	 * @throws TranslatorException
	 */
	public List<Object> performSearch(Select command) throws TranslatorException;

}
