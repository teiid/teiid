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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.util.HashCodeUtil;


public class VDBKey implements Serializable, Comparable<VDBKey>{
	private static final Integer[] EMPTY_PARTS = new Integer[3];

	private static final long serialVersionUID = -7249750823144856081L;
	
	private String name;
	private String baseName;
	private Integer[] versionParts;
    private int version;
    private int hashCode;
    private boolean fullySpecified = true;
    private boolean atMost;
    
    public static Pattern NAME_PATTERN = Pattern.compile("([^.]*)\\.v(?:(0|(?:[1-9]\\d{0,8})))(?:\\.(0|(?:[1-9]\\d{0,8})))?(?:\\.(0|(?:[1-9]\\d{0,8})))?(\\.)?$"); //$NON-NLS-1$
    
    public VDBKey(String name, int version) {
        this.name = name;
        Matcher m  = NAME_PATTERN.matcher(name);
        if (!m.matches()) {
        	this.baseName = name;
        	versionParts = EMPTY_PARTS;
        	fullySpecified = true;
        } else {
        	this.baseName = m.group(1);
        	versionParts = new Integer[3];
        	getPart(m, 0);
        	getPart(m, 1);
        	getPart(m, 2);
        	atMost = name.endsWith("."); //$NON-NLS-1$
        }
        this.version = version;
    }

	private void getPart(Matcher m, int part) {
		String val = m.group(part+2);
		if (val != null) {
			versionParts[part] = Integer.parseInt(val);
		} else {
			fullySpecified = false;
		}
	}    
    
    public String getName() {
		return name;
	}
    
    public String getBaseName() {
		return baseName;
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
    
    public String toString() {
    	if (version == 1) {
    		return name;
    	}
        return name + " " + version; //$NON-NLS-1$
    }

	@Override
	public int compareTo(VDBKey o) {
		int compare = String.CASE_INSENSITIVE_ORDER.compare(baseName, o.baseName);
		if (compare != 0) {
			return compare;
		}
		for (int i = 0; i < versionParts.length; i++) {
			compare = Integer.compare(versionParts[i]==null?0:versionParts[i], o.versionParts[i]==null?0:o.versionParts[i]);
			if (compare != 0) {
				return compare;
			}
		}
		compare = Integer.compare(versionParts.length, o.versionParts.length);
		if (compare != 0) {
			return compare;
		}
		compare = Integer.compare(version, o.version);
		if (compare != 0) {
			return compare;
		}
		return 0;
	}
	
	/**
	 * @return true if the vdb name was specified with a semantic version
	 */
	public boolean isSemantic() {
		return this.versionParts != EMPTY_PARTS;
	}
	
	/**
	 * @return true if all parts of the semantic version are specified
	 */
	public boolean isFullySpecified() {
		return fullySpecified;
	}
	
	/**
	 * @return true if the semantic version ends in a .
	 */
	public boolean isAtMost() {
		return atMost;
	}
	
	/**
	 * @param key
	 * @return true if the key version >= the current version
	 */
	public boolean acceptsVerion(VDBKey key) {
		for (int i = 0; i < versionParts.length; i++) {
			if (versionParts[i] == null) {
				break;
			}
			if (versionParts[i] != (key.versionParts[i]==null?0:key.versionParts[i])) {
				return false;
			}
		}
		return version <= key.version;
	}

}
