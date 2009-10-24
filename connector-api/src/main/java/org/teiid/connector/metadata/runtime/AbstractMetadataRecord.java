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

package org.teiid.connector.metadata.runtime;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.metamatrix.core.util.EquivalenceUtil;

/**
 * AbstractMetadataRecord
 */
public abstract class AbstractMetadataRecord implements Serializable {
	    
    public final static char NAME_DELIM_CHAR = '.';
    
    private String uuid; //globally unique id
    private String name; //contextually unique name
    private String fullName;
    
    private String nameInSource;
	
	private LinkedHashMap<String, String> properties;
	private String annotation;
	
	public String getUUID() {
		return uuid;
	}
	
	public void setUUID(String uuid) {
		this.uuid = uuid;
	}
	
	public String getNameInSource() {
		return nameInSource;
	}
	
	public void setNameInSource(String nameInSource) {
		this.nameInSource = nameInSource;
	}
	
	public String getFullName() {
        return this.fullName == null ? this.name : this.fullName;
	}
	
	public void setFullName(String fullName) {
		this.fullName = fullName;
	}
	
	public String getName() {
    	if(this.name == null || this.name.trim().length() == 0) {
			int nmIdx = this.fullName != null ? this.fullName.lastIndexOf(NAME_DELIM_CHAR) : -1;
			if (nmIdx == -1) {
				this.name = this.fullName;
			} else {
				this.name = this.fullName != null ? this.fullName.substring(nmIdx+1) : null;
			}
    	}
		return name;
	}	
	
	public void setName(String name) {
		this.name = name;
	}
	
    public String getModelName() {
		int prntIdx = fullName.indexOf(NAME_DELIM_CHAR);
		return fullName.substring(0, prntIdx);
    }

    public String toString() {
        return uuid + ": " + name; //$NON-NLS-1$
    }
    
    /**
     * @deprecated the returned value may be incorrect in the case of an XML element (see defects #11326 and #11362)
     */
    public String getParentFullName() {
        int prntIdx = getFullName() != null ? getFullName().lastIndexOf(NAME_DELIM_CHAR+getName()) : -1;
        if (prntIdx <= 0) {
            return ""; //$NON-NLS-1$
        }
        return getFullName().substring(0, prntIdx);
    }
    
    /**
     * Return the extension properties for this record - may be null
     * if {@link #setProperties(LinkedHashMap)} or {@link #setProperty(String, String)}
     * has not been called
     * @return
     */
    public Map<String, String> getProperties() {
    	if (properties == null) {
    		return Collections.emptyMap();
    	}
    	return properties;
	}
    
    public void setProperty(String key, String value) {
    	if (this.properties == null) {
    		this.properties = new LinkedHashMap<String, String>();
    	}
    	this.properties.put(key, value);
    }
    
    public void setProperties(LinkedHashMap<String, String> properties) {
		this.properties = properties;
	}

    public String getAnnotation() {
		return annotation;
	}
    
    public void setAnnotation(String annotation) {
		this.annotation = annotation;
	}

    /**
     * Compare two records for equality.
     */
    public boolean equals(Object obj) {

        if(obj == this) {
            return true;
        }

        if(obj.getClass() != this.getClass()) {
            return false;
        }

        AbstractMetadataRecord other = (AbstractMetadataRecord)obj;

        return EquivalenceUtil.areEqual(this.getUUID(), other.getUUID());
    }

    public int hashCode() {
        return this.uuid.hashCode();
    }
        
}