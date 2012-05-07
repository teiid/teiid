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

package org.teiid.translator.object.infinispan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.core.BundleUtil;
import org.teiid.language.Command;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectCacheConnection;
import org.teiid.translator.object.ObjectSourceProxy;

/** 
 * Represents an implementation for the connection to an Infinispan local cache data source. 
 */
public class InfinispanProxy implements ObjectSourceProxy { 
	
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(InfinispanProxy.class);
	
	private InfinispanObjectVisitor visitor = new InfinispanObjectVisitor();
	private InfinispanRemoteExecutionFactory factory;
	private ObjectCacheConnection connection;
	
	public InfinispanProxy(ObjectCacheConnection connection, InfinispanRemoteExecutionFactory factory) {
		this.factory = factory;
		this.connection = connection;
	}


	@Override
	public void close() {
		this.visitor = null;
		this.factory = null;
	}


	@Override
	public List<Object> get(Command command, String cache, String rootClassName) throws TranslatorException {
		visitor.visitNode(command);
		
		Class<?> rootClass = null;
		if (this.factory.getObjectMethodManager().getClassMethods(rootClassName) == null) {
			rootClass = this.factory.getObjectMethodManager().loadClassByName(rootClassName, null);
		} else {
			rootClass = this.factory.getObjectMethodManager().getClassMethods(rootClassName).getClassIdentifier();
		}
		try {
			return connection.get(visitor.getKeyCriteria(), cache, rootClass);
		} catch (Exception e) {
			throw new TranslatorException(e.getMessage());
		}
		
	}
	
	public  String formatColumnName(String columnName) {
		if (factory.isColumnNameFirstLetterUpperCase()) return columnName;
		
		return  columnName.substring(0, 1).toLowerCase() + columnName.substring(1);
	}

}
