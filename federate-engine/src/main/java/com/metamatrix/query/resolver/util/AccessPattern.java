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

package com.metamatrix.query.resolver.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * This class represents both virtual and physical access patterns.
 * 
 * If a virtual access pattern is initially unstatisfied, it may be
 * transformed by RuleMergeVirtual.  In this case, the history of the
 * access pattern will contain its previous definitions.
 */
public class AccessPattern implements Comparable, Cloneable {
    
    private Set unsatisfied = new HashSet();
    private LinkedList history = new LinkedList();
    
    public AccessPattern(Collection elements) {
        unsatisfied.addAll(elements);
        history.add(elements);
    }
    
    public Collection getCurrentElements() {
        return (Collection)history.getFirst();
    }
    
    public void addElementHistory(Collection elements) {
        this.history.addFirst(elements);
    }

    /** 
     * @return Returns the history.
     */
    public LinkedList getHistory() {
        return this.history;
    }
        
    /** 
     * @return Returns the unstaisfied.
     */
    public Set getUnsatisfied() {
        return this.unsatisfied;
    }
    
    /** 
     * @param unstaisfied The unstaisfied to set.
     */
    public void setUnsatisfied(Set unstaisfied) {
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

    /** 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(Object o) {
        AccessPattern other = (AccessPattern)o;
        
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
