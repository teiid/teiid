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

package com.metamatrix.vdb.runtime;

import com.metamatrix.core.util.HashCodeUtil;

public class VDBKey {

    private String name;
    private String version;
    
    public VDBKey(String name, String version) {
        this.name = name.toUpperCase();
        if (version != null) {
            this.version = version.toUpperCase();
        }
    }
    
    public String getName() {
		return name;
	}
    
    public String getVersion() {
		return version;
	}
    
    /** 
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return HashCodeUtil.hashCode(name.hashCode(), version);
    }
    
    /** 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        
        if (!(obj instanceof VDBKey)) {
            return false;
        }
        
        VDBKey other = (VDBKey)obj;
        
        if (!other.name.equals(this.name)) {
            return false;
        }
        
        if (this.version != null) {
            if (!this.version.equals(other.version)) {
                return false;
            }
        } else if (other.version != null){
            return false;
        }
        
        return true;
    }
    
    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return name + " " + version; //$NON-NLS-1$
    }
    
}
