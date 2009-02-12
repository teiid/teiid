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

package com.metamatrix.metadata.runtime.model;

import com.metamatrix.metadata.runtime.api.DataTypeID;
import com.metamatrix.metadata.runtime.api.ModelID;
public class BasicDataTypeID extends BasicMetadataID implements DataTypeID {
    private ModelID modelID = null;
    private String dtUUID;

/**
 * Call constructor to instantiate a BasicDataTypeID object for the fully qualified name and an internal unique identifier.
 */
    public BasicDataTypeID(String fullName, long internalUniqueID) {
        super(fullName, internalUniqueID);
    }

/**
 * Call constructor to instantiate a BasicDataTypeID object for the fully qualified name.
 */
    protected BasicDataTypeID(String fullName){
        super(fullName);
    }
/**
 * return the modelID this key is a part of.
 * @return ModelID is the model the key is contained in
 */
    public ModelID getModelID() {
        return modelID;
    }

     public void setModelID(ModelID id){
        this.modelID = id;
    }
	
	/**
	 * Override parent method to compare UID
	 */
	public boolean equals(Object obj) {
		boolean result = super.equals(obj);
		if(result){
			BasicMetadataID that = (BasicMetadataID) obj;
			return this.getUID() == that.getUID();	
		}
		return result;
	}
	
	/**
	 * Override parent method to compare UID
	 */
	public int compareTo(Object obj) {
		int result = super.compareTo(obj);
		if(result == 0){
			BasicMetadataID that = (BasicMetadataID) obj;
			result = (int)(this.getUID() - that.getUID());	
		}
		return result;
	}
	
	public String getUuid(){
        return this.dtUUID;
    }

    public void setUuid(String uuid){
        this.dtUUID = uuid;
    }
}

