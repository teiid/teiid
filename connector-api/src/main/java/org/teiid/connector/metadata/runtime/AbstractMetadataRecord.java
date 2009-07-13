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
import java.util.Collection;
import java.util.Properties;

import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.HashCodeUtil;

/**
 * AbstractMetadataRecord
 */
public abstract class AbstractMetadataRecord implements Serializable {
	    
    /**
	 * Constants for names of accessor methods that map to fields stored on the MetadataRecords.
	 * Note the names do not have "get" on them, this is also the nameInSource
	 * of the attributes on SystemPhysicalModel.
	 * @since 4.3
	 */
	public static interface MetadataFieldNames {
	    String RECORD_TYPE_FIELD    = "Recordtype"; //$NON-NLS-1$
	    String NAME_FIELD           = "Name"; //$NON-NLS-1$
	    String FULL_NAME_FIELD      = "FullName"; //$NON-NLS-1$
	    String MODEL_NAME_FIELD     = "ModelName"; //$NON-NLS-1$        
	    String UUID_FIELD           = "UUID"; //$NON-NLS-1$
	    String NAME_IN_SOURCE_FIELD = "NameInSource"; //$NON-NLS-1$
	    String PARENT_UUID_FIELD    = "ParentUUID"; //$NON-NLS-1$
	}

    public final static char NAME_DELIM_CHAR = '.';
    
	private String pathString;
	private String modelName;
    private char recordType;
    
    private String uuid;
    private String parentUUID;
    private String nameInSource;
    private String fullName;
	private String name;
	
	private Collection<PropertyRecordImpl> extensionProperties;
	private transient Properties properties;
	private AnnotationRecordImpl annotation;
	
	public String getUUID() {
		return uuid;
	}
	public void setUUID(String uuid) {
		this.uuid = uuid;
	}
	public String getParentUUID() {
		return parentUUID;
	}
	public void setParentUUID(String parentUUID) {
		this.parentUUID = parentUUID;
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
    
    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getModelName()
     */
    public String getModelName() {
    	if(this.modelName == null) {
			int prntIdx = getFullName() != null ? getFullName().indexOf(NAME_DELIM_CHAR) : -1;
			if (prntIdx <= 0) {
				this.modelName = getFullName();
			} else {
				this.modelName = getFullName() != null ? getFullName().substring(0, prntIdx) : null;
			}
    	}

    	return this.modelName;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getPathString()
     */
    public String getPathString() {
		if(this.pathString == null) {
			this.pathString = getFullName() != null ? getFullName().replace(NAME_DELIM_CHAR, FileUtils.SEPARATOR) : null;			
		}
		return this.pathString;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getPath()
     */
    public String getPath() {
        return getPathString();
    }
    
    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getParentFullName()
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
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getParentPathString()
     * @deprecated the returned value may be incorrect in the case of an XML element (see defects #11326 and #11362)
     */
    public String getParentPathString() {
        String parentFullName = getParentFullName();
        return parentFullName != null ? parentFullName.replace(NAME_DELIM_CHAR, FileUtils.SEPARATOR) : null;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getRecordType()
     */
    public char getRecordType() {
        return this.recordType;
    }

    /**
     * @param c
     */
    public void setRecordType(char c) {
        recordType = c;
    }

    public String toString() {
        return getFullName();
    }
    
    public Collection<PropertyRecordImpl> getExtensionProperties() {
		return extensionProperties;
	}
    
    public void setExtensionProperties(Collection<PropertyRecordImpl> properties) {
		this.extensionProperties = properties;
	}
    
    public Properties getProperties() {
		return properties;
	}
    
    public void setProperties(Properties properties) {
		this.properties = properties;
	}

    public AnnotationRecordImpl getAnnotation() {
		return annotation;
	}
    
    public void setAnnotation(AnnotationRecordImpl annotation) {
		this.annotation = annotation;
	}

    /**
     * Compare two records for equality.
     */
    public boolean equals(Object obj) {

        if(obj == this) {
            return true;
        }

        if(obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        AbstractMetadataRecord other = (AbstractMetadataRecord)obj;

        if(this.getRecordType() != other.getRecordType()) { return false; }        
        if(!EquivalenceUtil.areEqual(this.getUUID(), other.getUUID())) { return false; }
        if(!EquivalenceUtil.areEqual(this.getParentUUID(), other.getParentUUID())) { return false; }
        if(!EquivalenceUtil.areEqual(this.getFullName(), other.getFullName())) { return false; }
        if(!EquivalenceUtil.areEqual(this.getNameInSource(), other.getNameInSource())) { return false; }

        return true;
    }

    /**
     * WARNING: The hash code relies on the variables
     * in the record, so changing the variables will change the hash code, causing
     * a select to be lost in a hash structure.  Do not hash a record if you plan
     * to change it.
     */
    public int hashCode() {
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.recordType);
        myHash = HashCodeUtil.hashCode(myHash, this.getFullName());
        myHash = HashCodeUtil.hashCode(myHash, this.getUUID());
        myHash = HashCodeUtil.hashCode(myHash, this.getParentUUID());
        myHash = HashCodeUtil.hashCode(myHash, this.getNameInSource());
        return myHash;
    }
        
}