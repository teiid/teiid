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

import java.util.LinkedList;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.visitor.SQLStringVisitor;

/**
 * <p>This is a subclass of Symbol representing multiple output columns.</p>
 */
public class MultipleElementSymbol implements SelectSymbol {
    private List<ElementSymbol> elementSymbols;
    private GroupSymbol group;

    public MultipleElementSymbol() {
    }
    
    /**
     * Construct a multiple element symbol
     * @param name Name of the symbol
     */
    public MultipleElementSymbol(String name){
        this.group = new GroupSymbol(name);
    }

    /**
     * Set the {@link ElementSymbol}s that this symbol refers to
     * @param elementSymbols List of {@link ElementSymbol}
     */
    public void setElementSymbols(List<ElementSymbol> elementSymbols){
        this.elementSymbols = elementSymbols;
    }

    /**
     * Get the element symbols referred to by this multiple element symbol
     * @return List of {@link ElementSymbol}s, may be null
     */
    public List<ElementSymbol> getElementSymbols(){
        return this.elementSymbols;
    }

    /**
     * Add an element symbol referenced by this multiple element symbol
     * @param symbol Element symbol referenced by this multiple element symbol
     */
    public void addElementSymbol(ElementSymbol symbol) {
		if(getElementSymbols() == null) { 
			setElementSymbols(new LinkedList<ElementSymbol>());
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
    
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

	/**
	 * Return a deep copy of this object
	 * @return Deep copy of this object
	 */
	public Object clone() {
		MultipleElementSymbol copy = new MultipleElementSymbol();
		if (group != null) {
			copy.group = group.clone();
		}

		List<ElementSymbol> elements = getElementSymbols();
		if(elements != null && elements.size() > 0) {
			copy.setElementSymbols(LanguageObject.Util.deepClone(elements, ElementSymbol.class));				
		}	

		return copy;
	}
	
	/**
	 * @return null if selecting all groups, otherwise the specific group
	 */
	public GroupSymbol getGroup() {
		return group;
	}
	
	public void setGroup(GroupSymbol group) {
		this.group = group;
	}
	
	@Override
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.hashCode(0, group);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof MultipleElementSymbol)) {
			return false;
		}
		MultipleElementSymbol other = (MultipleElementSymbol)obj;
		return EquivalenceUtil.areEqual(this.group, other.group);
	}

}
