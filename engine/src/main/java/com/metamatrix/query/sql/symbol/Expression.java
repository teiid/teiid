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

package com.metamatrix.query.sql.symbol;

import com.metamatrix.query.sql.LanguageObject;

/**
 * This is the interface for an expression in a SQL string.  Expressions can be of several
 * types (see subclasses), but all expressions have a type.  These types are used for 
 * type checking.
 */
public interface Expression extends LanguageObject {

	/**
	 * Return true if expression has been fully resolved.  Typically the QueryResolver component
	 * will handle resolution of an expression.
	 * @return True if resolved
	 */
	boolean isResolved();
	
	/**
	 * Get the return type of this expression.  This method will not necessarily work right 
	 * before resolution ({@link #isResolved()} returns true).
	 * @return Java class name
	 */
	Class getType();
		
}
