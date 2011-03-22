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

package org.teiid.query.sql.symbol;



/**
 * <p>This is a subclass of Symbol representing a single output column.</p>
 */
public abstract class SingleElementSymbol extends SelectSymbol implements Expression {

	/**
     * Character used to delimit name components in a symbol
	 */
    public static final String SEPARATOR = "."; //$NON-NLS-1$

    /**
     * Passthrough constructor used for cloning 
     * @param name
     * @param canonicalName
     * @param hashcode
     * @since 4.3
     */
    protected SingleElementSymbol(String name, String canonicalName) {
        super(name, canonicalName);
    }
    
    /**
     * Construct a symbol with a name
     * @param name Name of symbol
     */
    public SingleElementSymbol(String name){
        super(name);
    }

    public static String getShortName(String name) {
        int index = name.lastIndexOf(SEPARATOR);
        if(index >= 0) { 
            return name.substring(index+1);
        }
        return name;
    }

}
