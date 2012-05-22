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

import java.util.List;

import org.teiid.core.BundleUtil;
import org.teiid.language.Command;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectVisitor;
import org.teiid.translator.object.ObjectSourceProxy;

/** 
 * Represents an implementation for the connection to an Infinispan local cache data source. 
 */
public class InfinispanProxy implements ObjectSourceProxy { 
	
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(InfinispanProxy.class);
	
	private InfinispanRemoteExecutionFactory factory;
	private InfinispanCacheConnection connection;
	
	public InfinispanProxy(InfinispanCacheConnection connection, InfinispanRemoteExecutionFactory factory) {
		this.factory = factory;
		this.connection = connection;
	}


	@Override
	public void close() {
		this.factory = null;
		this.connection = null;
	}


	@Override
	public List<Object> get(Command command, String cache, ObjectVisitor visitor) throws TranslatorException {
		
		Class<?> rootClass = null;
		if (this.factory.getObjectMethodManager().getClassMethods(visitor.getRootNodeClassName()) == null) {
			rootClass = this.factory.getObjectMethodManager().loadClassByName(visitor.getRootNodeClassName(), null);
		} else {
			rootClass = this.factory.getObjectMethodManager().getClassMethods(visitor.getRootNodeClassName()).getClassIdentifier();
		}
		try {
			return connection.get(visitor.getCriterion(), cache, rootClass);
		} catch (Exception e) {
			throw new TranslatorException(e.getMessage());
		}
		
	}
	
	public  String formatColumnName(String columnName) {
		if (factory.isColumnNameFirstLetterUpperCase()) return columnName;
		
		return  columnName.substring(0, 1).toLowerCase() + columnName.substring(1);
	}

}
