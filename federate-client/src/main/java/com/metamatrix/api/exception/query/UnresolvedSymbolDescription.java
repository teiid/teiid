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

package com.metamatrix.api.exception.query;

import java.io.Serializable;

/**
 * This helper object describes an unresolved symbol found during
 * query resolution.
 */
public class UnresolvedSymbolDescription implements Serializable {

	private String symbol;
	private String description;

    /**
     * Construct a description given the symbol and it's description.
     * @param symbol Unresolved symbol
     * @param description Description of error
     */	
	public UnresolvedSymbolDescription(String symbol, String description) {
		this.symbol = symbol;
		this.description = description;
	}
	
    /**
     * Get the symbol that was unresolved
     * @return Unresolved symbol
     */
	public String getSymbol() {
		return this.symbol;
	}
	
    /**
     * Get the description of the problem
     * @return Problem description
     */
	public String getDescription() {
		return this.description;
	}	
	
    /**
     * Get string representation of the unresolved symbol description
     * @return String representation
     */
	public String toString() {
		StringBuffer str = new StringBuffer();
		if(symbol != null) { 
			str.append("Unable to resolve '"); //$NON-NLS-1$
			str.append(symbol);
			str.append("': "); //$NON-NLS-1$
		}
		if(description != null) { 
			str.append(description);
		} else {
			str.append("Unknown reason"); //$NON-NLS-1$
		}
		return str.toString();
	}

}
