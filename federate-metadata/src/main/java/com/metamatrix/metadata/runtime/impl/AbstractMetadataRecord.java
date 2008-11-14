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

package com.metamatrix.metadata.runtime.impl;

import java.util.HashMap;
import java.util.Map;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.id.ObjectID;
import com.metamatrix.core.id.UUID;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.metadata.runtime.RuntimeMetadataPlugin;
import com.metamatrix.modeler.core.container.EObjectFinder;
import com.metamatrix.modeler.core.index.IndexConstants;
import com.metamatrix.modeler.core.metadata.runtime.MetadataRecord;

/**
 * AbstractMetadataRecord
 */
public abstract class AbstractMetadataRecord implements MetadataRecord {
	    
    private static final int DEFAULT_INDEX_VERSION = 0;
    
	private String pathString;
	private String modelName;
    private String resourcePath;
    private int indexVersion = DEFAULT_INDEX_VERSION;
    private char recordType;
    private transient Map propValues;
    
	protected Object eObject;
	protected MetadataRecordDelegate delegate;
	private EObjectFinder finder;

    // ==================================================================================
    //                        C O N S T R U C T O R S
    // ==================================================================================

    public AbstractMetadataRecord() {}

    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getUUID()
     */
    public String getUUID() {
    	return this.delegate.getUUID();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getParentUUID()
     */
    public String getParentUUID() {
		return this.delegate.getParentUUID();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getNameInSource()
     */
    public String getNameInSource() {
		return this.delegate.getNameInSource();
    }
    
    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getModelName()
     */
    public String getModelName() {
    	if(this.modelName == null) {
			int prntIdx = getFullName() != null ? getFullName().indexOf(IndexConstants.NAME_DELIM_CHAR) : -1;
			if (prntIdx <= 0) {
				this.modelName = getFullName();
			} else {
				this.modelName = getFullName() != null ? getFullName().substring(0, prntIdx) : null;
			}
    	}

    	return this.modelName;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getFullName()
     */
    public String getFullName() {
		return this.delegate.getFullName();
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getName()
     */
    public String getName() {
		return this.delegate.getName();
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getPathString()
     */
    public String getPathString() {
		if(this.pathString == null) {
			this.pathString = getFullName() != null ? getFullName().replace(IndexConstants.NAME_DELIM_CHAR, FileUtils.SEPARATOR) : null;			
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
        int prntIdx = getFullName() != null ? getFullName().lastIndexOf(IndexConstants.NAME_DELIM_CHAR+getName()) : -1;
        if (prntIdx <= 0) {
            return StringUtil.Constants.EMPTY_STRING;
        }
        return getFullName().substring(0, prntIdx);
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getParentPathString()
     * @deprecated the returned value may be incorrect in the case of an XML element (see defects #11326 and #11362)
     */
    public String getParentPathString() {
        String parentFullName = getParentFullName();
        return parentFullName != null ? parentFullName.replace(IndexConstants.NAME_DELIM_CHAR, FileUtils.SEPARATOR) : null;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getRecordType()
     */
    public char getRecordType() {
        return this.recordType;
    }

    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    /**
     * @param string
     */
    public void setNameInSource(String string) {
    	this.delegate.setNameInSource(string);
    }

    /**
     * @param path
     */
    public void setFullName(String fullName) {
    	this.delegate.setFullName(fullName);
    }

	/**
	 * @param String
	 */
	public void setName(String name) {
		this.delegate.setName(name);
	}

    /**
     * @param string
     */
    public void setUUID(String string) {
        this.delegate.setUUID(string);
    }

    /**
     * @param string
     */
    public void setParentUUID(String string) {
        this.delegate.setParentUUID(string);        
    }

    /**
     * @return index version number
     */
    public int getIndexVersion() {
        return this.indexVersion;
    }

    /**
     * @param int
     */
    public void setIndexVersion(final int version) {
        this.indexVersion = version;
    }

    /** 
     * @param container The container to set.
     * @since 4.2
     */
    public void setEObjectFinder(EObjectFinder finder) {
        this.finder = finder;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getEObject()
     */
    public Object getEObject() {
        if(eObject != null) {
            return eObject;    
        }
        Assertion.isNotNull(this.finder);
        try {
            // get the Object ID given a UUID
            String uuid = getUUID();
            int delimitIndex = uuid.indexOf(UUID.DELIMITER);
            ObjectID objID = UUID.stringToObject(uuid.substring(delimitIndex+1));
            // Return the EObject instance found in the ModelContainer ...
            return this.finder.find(objID);
        } catch(Exception e) {
            LogManager.logWarning(RuntimeMetadataPlugin.PLUGIN_ID, e, e.getMessage());   
        }
        return null;
    }

    /**
     * @return
     */
    public String getResourcePath() {
        return resourcePath;
    }

    /**
     * @param path
     */
    public void setResourcePath(String path) {
        resourcePath = path;
    }

    /**
     * @param c
     */
    public void setRecordType(char c) {
        recordType = c;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getPropertyValue(java.lang.String)
     */
    public Object getPropertyValue(String propertyName) {
        ArgCheck.isNotNull(propertyName);
        if(propValues != null) {
            return propValues.get(propertyName);
        }
        return null;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#setPropertyValue(java.lang.String, java.lang.Object)
     */
    public void setPropertyValue(String propertyName, Object propertyVame) {
		if(propertyName != null && propertyVame != null) {
			if(propValues == null) {
				propValues = new HashMap();
			}
			propValues.put(propertyName, propertyVame);
		}
    }

    public String toString() {
        return getFullName();
    }

    // ==================================================================================
    //                      P R O T E C T E D  M E T H O D S
    // ==================================================================================

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
     * Get hashcode for From.  WARNING: The hash code relies on the variables
     * in the record, so changing the variables will change the hash code, causing
     * a select to be lost in a hash structure.  Do not hash a record if you plan
     * to change it.
     */
    public int hashCode() {
        int myHash = 0;
        myHash = HashCodeUtil.hashCode(myHash, this.recordType);
        myHash = HashCodeUtil.hashCode(myHash, this.delegate.getFullName());
        myHash = HashCodeUtil.hashCode(myHash, this.delegate.getUUID());
        myHash = HashCodeUtil.hashCode(myHash, this.delegate.getParentUUID());
        myHash = HashCodeUtil.hashCode(myHash, this.delegate.getNameInSource());
        return myHash;
    }
    
}