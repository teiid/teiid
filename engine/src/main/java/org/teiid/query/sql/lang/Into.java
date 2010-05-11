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

/*
 */
package org.teiid.query.sql.lang;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Rpresent INTO clause in SELECT ... INTO ... clause, which is used to create
 * temporary table.
 */
public class Into implements LanguageObject {
    private GroupSymbol group;
    
    /**
     * Construct default object
     */
    public Into() {
    }
        
    /**
     * Construct object with specified group
     * @param group Group being held
     */
    public Into(GroupSymbol group) {
        this.group = group;
    }
        
    /**
     * Set the group held by the clause
     * @param group Group to hold
     */
    public void setGroup(GroupSymbol group) {
        this.group = group;
    } 
        
    /**
     * Get group held by clause
     * @return Group held by clause
     */
    public GroupSymbol getGroup() {
        return this.group;
    }
        
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Check whether objects are equal
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        } 
            
        if(! (obj instanceof Into)) { 
            return false;
        }       
        
        return EquivalenceUtil.areEqual(getGroup(), ((Into)obj).getGroup());
    }
        
    /**
     * Get hash code of object
     * @return Hash code
     */
    public int hashCode() {
        if(this.group == null) { 
            return 0;
        }
        return this.group.hashCode();
    }
        
    /**
     * Get deep clone of object
     * @return Deep copy of the object
     */
    public Object clone() {
        GroupSymbol copyGroup = null;
        if(this.group != null) { 
            copyGroup = (GroupSymbol) this.group.clone();
        }
        return new Into(copyGroup);  
    }
    
    
    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }
            
}
