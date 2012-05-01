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

package org.teiid.vdb.runtime;

import java.io.Serializable;

import org.teiid.core.util.HashCodeUtil;


public class VDBKey implements Serializable, Comparable<VDBKey>{
	private static final long serialVersionUID = -7249750823144856081L;
	
	private String name;
    private int version;
    private int hashCode;
    
    public VDBKey(String name, String version) {
        this.name = name;
        if (version != null) {
            this.version = Integer.parseInt(version);
        }
    }
    
    public VDBKey(String name, int version) {
        this.name = name;
        this.version = version;
    }    
    
    public String getName() {
		return name;
	}
    
    public int getVersion() {
		return version;
	}
    
    /** 
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
    	if (hashCode == 0) {
    		hashCode = HashCodeUtil.hashCode(HashCodeUtil.expHashCode(name, false), version); 
    	}
        return hashCode;
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
        
        return version == other.version 
        && other.name.equalsIgnoreCase(this.name);
    }
    
    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return name + " " + version; //$NON-NLS-1$
    }

	@Override
	public int compareTo(VDBKey o) {
		int compare = String.CASE_INSENSITIVE_ORDER.compare(name, o.name);
		if (compare == 0) {
			return version - o.version;
		}
		return compare;
	}
	
}
