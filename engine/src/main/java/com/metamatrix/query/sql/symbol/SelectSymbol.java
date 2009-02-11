/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

/** 
 * This is the server's representation of a metadata symbol that can be used
 * in a SELECT statement.  It exists as a typing mechanism and to provide a single
 * additional method to get an ordered list of elements from each symbol.
 */
public abstract class SelectSymbol extends Symbol {

    /**
     * Passthrough constructor used for cloning 
     * @param name
     * @param canonicalName
     * @since 4.3
     */
    protected SelectSymbol(String name, String canonicalName) {
        super(name, canonicalName);
    }
    
	/**
	 * Construct a symbol with a name
	 * @param name Name of symbol
	 */
	public SelectSymbol(String name) {
		super(name);
	}
				
}
