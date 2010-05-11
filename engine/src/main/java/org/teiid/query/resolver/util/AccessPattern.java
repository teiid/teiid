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

package org.teiid.query.resolver.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.teiid.query.sql.symbol.ElementSymbol;


/**
 * This class represents both virtual and physical access patterns.
 * 
 * If a virtual access pattern is initially unsatisfied, it may be
 * transformed by RuleMergeVirtual.  In this case, the history of the
 * access pattern will contain its previous definitions.
 */
public class AccessPattern implements Comparable<AccessPattern>, Cloneable {
    
    private Set<ElementSymbol> unsatisfied = new HashSet<ElementSymbol>();
    private LinkedList<Collection<ElementSymbol>> history = new LinkedList<Collection<ElementSymbol>>();
    
    public AccessPattern(Collection<ElementSymbol> elements) {
        unsatisfied.addAll(elements);
        history.add(elements);
    }
    
    public Collection<ElementSymbol> getCurrentElements() {
        return history.getFirst();
    }
    
    public void addElementHistory(Collection<ElementSymbol> elements) {
        this.history.addFirst(elements);
    }

    /** 
     * @return Returns the history.
     */
    public LinkedList<Collection<ElementSymbol>> getHistory() {
        return this.history;
    }
        
    /** 
     * @return Returns the unsatisfied.
     */
    public Set<ElementSymbol> getUnsatisfied() {
        return this.unsatisfied;
    }
    
    /** 
     * @param unstaisfied The unsatisfied to set.
     */
    public void setUnsatisfied(Set<ElementSymbol> unstaisfied) {
        this.unsatisfied = unstaisfied;
    }
    
    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Access Pattern: Unsatisfied "); //$NON-NLS-1$
        sb.append(unsatisfied);
        sb.append(" History "); //$NON-NLS-1$
        sb.append(history);
        return sb.toString();
    }
    
    @Override
    public int compareTo(AccessPattern other) {
        if (this.unsatisfied.size() > other.unsatisfied.size()){
            return 1;
        } else if (this.unsatisfied.size() < other.unsatisfied.size()){
            return -1;
        }
        return 0;
    }
    
    /** 
     * @see java.lang.Object#clone()
     */
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException err) {
            return null;
        }
    }
    
}
