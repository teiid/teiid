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

package org.teiid.translator.jdbc;

import org.hibernate.dialect.Dialect;

/**
 * A pruned version of a Hibernate {@link Dialect} for use by Teiid
 */
public interface SQLDialect {
	
	String getCreateTemporaryTableString();
	
	String getCreateTemporaryTablePostfix();
	
	String getDropTemporaryTableString();
	
	//TODO: there's a chance that the type is not supported by the source 
	//which will throw a HibernateException - this is likely a modeling error
	//rather than something we need to generally consider
	public String getTypeName(int code, long length, int precision, int scale);
	
	boolean supportsTemporaryTables();

}
