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

package org.teiid.metadata;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;


/**
 * AbstractMetadataRecord
 */
public abstract class AbstractMetadataRecord implements Serializable {
	
	private static final long serialVersionUID = 564092984812414058L;

	public final static char NAME_DELIM_CHAR = '.';
	
	private static AtomicLong UUID_SEQUENCE = new AtomicLong();
    
    private String uuid; //globally unique id
    private String name; //contextually unique name
    private String canonicalName;
    
    private String nameInSource;
	
	private LinkedHashMap<String, String> properties;
	private String annotation;
	
	public String getUUID() {
		if (uuid == null) {
			uuid = String.valueOf(UUID_SEQUENCE.getAndIncrement());
		}
		return uuid;
	}
	
	public void setUUID(String uuid) {
		this.uuid = uuid;
	}
	
	public String getNameInSource() {
		return nameInSource;
	}
	
	public void setNameInSource(String nameInSource) {
		this.nameInSource = DataTypeManager.getCanonicalString(nameInSource);
	}
	
	/**
     * WARNING - The name returned by this method may be ambiguous and
     * is not SQL safe - it may need quoted/escaped
     */
	public String getFullName() {
        AbstractMetadataRecord parent = getParent();
        if (parent != null) {
        	String result = parent.getFullName() + NAME_DELIM_CHAR + getName();
        	return result;
        }
        return name;
	}
	
	public AbstractMetadataRecord getParent() {
		return null;
	}
	
	public String getName() {
		return name;
	}	
	
	public void setName(String name) {
		this.name = DataTypeManager.getCanonicalString(name);
		this.canonicalName = DataTypeManager.getCanonicalString(name.toUpperCase());
	}
	
	public String getCanonicalName() {
		return canonicalName;
	}
	
    public String toString() {
    	StringBuffer sb = new StringBuffer(100);
        sb.append(getClass().getSimpleName());
        sb.append(" name="); //$NON-NLS-1$
        sb.append(getName());
        sb.append(", nameInSource="); //$NON-NLS-1$
        sb.append(getNameInSource());
        sb.append(", uuid="); //$NON-NLS-1$
        sb.append(getUUID());
        return sb.toString();
    }
    
    /**
     * Return the extension properties for this record - may be unmodifiable
     * if {@link #setProperties(LinkedHashMap)} or {@link #setProperty(String, String)}
     * has not been called.
     * @return
     */
    public Map<String, String> getProperties() {
    	if (properties == null) {
    		return Collections.emptyMap();
    	}
    	return properties;
	}
    
    /**
     * The preferred setter for extension properties.
     * @param key
     * @param value
     */
    public void setProperty(String key, String value) {
    	if (this.properties == null) {
    		this.properties = new LinkedHashMap<String, String>();
    	}
    	this.properties.put(DataTypeManager.getCanonicalString(key), DataTypeManager.getCanonicalString(value));
    }
    
    public void setProperties(LinkedHashMap<String, String> properties) {
		this.properties = properties;
	}

    public String getAnnotation() {
		return annotation;
	}
    
    public void setAnnotation(String annotation) {
		this.annotation = DataTypeManager.getCanonicalString(annotation);
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
        return getUUID().hashCode();
    }
        
}