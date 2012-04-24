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
package org.teiid.translator.object.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.teiid.language.Command;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectSourceProxy;

public class MapCacheProxy implements ObjectSourceProxy {
	private MapCacheObjectVisitor visitor = new MapCacheObjectVisitor();
	private Object connection;
	private MapCacheExecutionFactory factory;
	

	public MapCacheProxy(Object connection, MapCacheExecutionFactory factory) {
		this.connection = connection;
		this.factory = factory;
	}

	
	private Map<Object, Object> getCache() {
		return factory.getCache();
	}


	@Override
	public List<Object> get(Command command) throws TranslatorException {
		visitor.visitNode(command);

		List<Object> results = null;
		if (visitor.compare) {
			results = new ArrayList<Object>(1);
			results.add(getCache().get(visitor.value));
			return results;
			
		} else {
			results = new ArrayList<Object>();
			results.addAll(getCache().values());
			return results;
			
		}
	}
	


	@Override
	public void close() {
		
	}

	
}
