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
 * ObjectSourceProxy interface is how the translator will ask for objects from the cache based on
 * the {@link Command}. The visitor will provide a parsed query such that the
 * the proxy implementor can use the criteria to perform vendor specific logic to obtain
 * objects from the cache. 
 * </p> 
 * <p>
 * The specific proxy implementation will be instantiated by  {@link ObjectExecutionFactory}.
 * Passing in the connection object and ObjectExecutionFactory.
 * </P
 * 
 * @author vhalbert
 *
 */

public interface ObjectSourceProxy {
		
	/**
	 * Called by {@link ObjectExecution}, passing in the sql <code>command</code>, to obtain the objects from 
	 * the cache based.  The implementor will need to parse the command and interpret the criteria according
	 * to data source query syntax.
	 * @param command is the SELECT command to query the data source
	 * @return List of objects found in the cache.
	 * @throws TranslatorException is thrown if there are issues querying the data source
	 */
	List<Object> get(Command command) throws TranslatorException;

	
	/**
	 * Called when this instance has completed its logical task.  This may include closing the connection, if that makes sence.
	 */
	void close();

}
