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

import org.teiid.language.Command;
import org.teiid.translator.TranslatorException;

/**
 * <p>
 * ObjectSourceProxy interface is implemented for a specific Object data source.  That
 * implementation will be responsible for adding its query logic for translating the
 * {@link Command} into vendor specific syntax for querying the cache.
 * </p> 
 * 
 * @author vhalbert
 *
 */

public interface ObjectSourceProxy {
		
	/**
	 * Called by {@link ObjectExecution}, passing in the sql {@link Command command}, the cache to be
	 * queries and the <code>rootClassName</code> to identify the object type to be obtained from
	 * the cachee.  The implementor will need to parse the command and interpret the criteria according
	 * to data source query syntax.
	 * @param command is the SELECT command to query the data source
	 * @param cacheName is the name of the cache to query
	 * @param visitor represents the source tables and columns being queried
	 * @return List of objects found in the cache.
	 * @throws TranslatorException is thrown if there are issues querying the data source
	 */
	List<Object> get(Command command, String cacheName, ObjectVisitor visitor) throws TranslatorException;

	
	/**
	 * Called when this instance has completed its logical task.  This may include closing the connection, if that makes sence.
	 */
	void close();

}
