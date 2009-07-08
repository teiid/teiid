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

import java.util.ArrayList;
import java.util.List;

import org.teiid.connector.metadata.runtime.MetadataConstants;

import com.metamatrix.metadata.runtime.api.ElementID;
import com.metamatrix.metadata.runtime.api.Key;
import com.metamatrix.metadata.runtime.api.MetadataID;

public class BasicKey extends BasicMetadataObject implements Key {
        private String description;
        private boolean isPrimaryKey;
        private boolean isForeignKey;
        private boolean isAccessPattern;
        private boolean isIndexed;
        private short matchType = MetadataConstants.NOT_DEFINED_SHORT;
        private boolean isUniqueKey;
        private short keyType;
        private MetadataID referencedKey;
        private transient List elements;
        private String alias;
        private long referencedKeyUid;
        private String path;

/**
 * Call constructor to instantiate a runtime object by passing the RuntimeID that identifies the entity and the VIrtualDatabaseID that identifes the Virtual Database the object will be contained.
 */
    public BasicKey(BasicKeyID keyID, BasicVirtualDatabaseID virtualDBID) {
        super(keyID, virtualDBID);
    }

    public String getDescription() {
        return this.description;
    }

    /**
    * Override the super method so that when the name
    * is returned, it is the name and not the full path for
    * a key
    */
    public String getNameInSource() {
        String alias = getAlias();
        if(alias != null)
	        return alias;
        return getName();
    }
    public String getAlias(){
        return alias;
    }
    public void setAlias(String alias){
        this.alias = alias;
    }
    public List getElementIDs() {
	      return elements;
    }
    public MetadataID getReferencedKey() {
        return this.referencedKey;
    }
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }
    public boolean isForeignKey() {
        return isForeignKey;
    }
    public boolean isUniqueKey() {
        return isUniqueKey;
    }
    public boolean isIndexed() {
        return isIndexed;
    }
    public boolean isAccessPattern() {
        return isAccessPattern;
    }
    public short getKeyType() {
        return keyType;
    }
    public short getMatchType() {
        return matchType;
    }
    public long getReferencedKeyUID() {
	      return this.referencedKeyUid;
    }
     public String getPath() {
	      return this.path;
    }
    public void setDescription(String description) {
        this.description = description;
    }
    public void setIsIndexed(boolean isIndexed) {
        this.isIndexed = isIndexed;
    }
    public void setKeyType(short keyType) {
        this.keyType = keyType;
        if(keyType == MetadataConstants.KEY_TYPES.PRIMARY_KEY){
            this.isPrimaryKey = true;
        }else if(keyType == MetadataConstants.KEY_TYPES.FOREIGN_KEY){
            this.isForeignKey = true;
        }else if(keyType == MetadataConstants.KEY_TYPES.UNIQUE_KEY){
            this.isUniqueKey = true;
        }else if(keyType == MetadataConstants.KEY_TYPES.ACCESS_PATTERN){
            this.isAccessPattern = true;
        }else{
            //non-unique key
        }
    }
    public void setMatchType(short matchType) {
       this.matchType = matchType;
    }
    public void setReferencedKey(MetadataID referKey) {
	      this.referencedKey = referKey;
    }
    public void setReferencedKeyUID(long uid) {
	      this.referencedKeyUid = uid;
    }

    public void setElementIDs(List elements) {
	      this.elements = elements;
    }
    public void clearElementIDs() {
        if(this.elements != null) {
            this.elements.clear();
        }
    }
    public void setPath(String path) {
	      this.path = path;
    }
    public void addElementID(ElementID elementID){
        if(elements == null)
            elements = new ArrayList();
        elements.add(elementID);
    }
}

