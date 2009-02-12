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

import java.util.*;

/**
 * <p>This is a subclass of Symbol representing multiple output columns.</p>
 */
public abstract class MultipleElementSymbol extends SelectSymbol {
    private List elementSymbols;

    /**
     * Passthrough constructor used for cloning 
     * @param name
     * @param canonicalName
     * @since 4.3
     */
    protected MultipleElementSymbol(String name, String canonicalName) {
        super(name, canonicalName);
    }
    
    /**
     * Construct a multiple element symbol
     * @param name Name of the symbol
     */
    public MultipleElementSymbol(String name){
        super(name);
    }

    /**
     * Set the {@link ElementSymbol}s that this symbol refers to
     * @param elementSymbols List of {@link ElementSymbol}
     */
    public void setElementSymbols(List elementSymbols){
        this.elementSymbols = elementSymbols;
    }

    /**
     * Get the element symbols referred to by this multiple element symbol
     * @return List of {@link ElementSymbol}s, may be null
     */
    public List getElementSymbols(){
        return this.elementSymbols;
    }

    /**
     * Add an element symbol referenced by this multiple element symbol
     * @param symbol Element symbol referenced by this multiple element symbol
     */
    public void addElementSymbol(ElementSymbol symbol) {
		if(getElementSymbols() == null) { 
			setElementSymbols(new LinkedList());
		}
		getElementSymbols().add(symbol);
    }
	
    /**
     * True if multiple element symbol has been resolved by having all referring element
     * symbols set.
     * @return True if symbol has been resolved
     */
    public boolean isResolved() {
        return(elementSymbols != null);
    }

}
