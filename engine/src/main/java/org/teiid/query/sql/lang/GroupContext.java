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

package org.teiid.query.sql.lang;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.symbol.GroupSymbol;


/**
 *  A GroupContext represents a set of groups in a hierarchy that determines
 *  resolving order.
 */
public class GroupContext implements Cloneable {
    
    private Collection<GroupSymbol> groups;
    
    private GroupContext parent;
    
    public GroupContext() {
        this(null, null);
    }
    
    public GroupContext(GroupContext parent, Collection<GroupSymbol> groups) {
        this.parent = parent;
        if (groups == null) {
            this.groups = new LinkedList<GroupSymbol>();
        } else {
            this.groups = groups;    
        }
    }

    public Collection<GroupSymbol> getGroups() {
        return this.groups;
    }
    
    public void addGroup(GroupSymbol symbol) {
        this.groups.add(symbol);
    }

    public GroupContext getParent() {
        return this.parent;
    }
    
    /**
     * Flattens all contexts to a single list
     *  
     * @return
     */
    public List<GroupSymbol> getAllGroups() {
        LinkedList<GroupSymbol> result = new LinkedList<GroupSymbol>();
        
        GroupContext root = this;
        while (root != null) {
            result.addAll(root.getGroups());
            root = root.getParent();
        }

        return result;
    }
    
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException err) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30446, err);
        }
    }
    
    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        String result = groups.toString(); 
        
        if (parent != null) {
            result += "\n" + parent.toString(); //$NON-NLS-1$
        }
        
        return result;
    }
    
}
