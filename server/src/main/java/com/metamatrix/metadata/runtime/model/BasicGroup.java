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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.teiid.connector.metadata.runtime.MetadataConstants;

import com.metamatrix.metadata.runtime.api.ElementID;
import com.metamatrix.metadata.runtime.api.Group;
import com.metamatrix.metadata.runtime.api.KeyID;
//import com.metamatrix.metadata.runtime.api.MetadataConstants;
import com.metamatrix.metadata.runtime.api.MetadataID;




public class BasicGroup extends BasicMetadataObject implements Group {

        private String path;
        private String description;
        private boolean isPhysical = true;
        private boolean isSystem = false;
        private short tableType = MetadataConstants.NOT_DEFINED_SHORT;

        private String queryPlan;
        private boolean hasQueryPlan;
        
        private String updateQueryPlan;
        private boolean hasUpdateQueryPlan;
        private boolean updateAllowed;
        
        private String insertQueryPlan;
        private boolean hasInsertQueryPlan;
        private boolean insertAllowed;
        
        private String deleteQueryPlan;
        private boolean hasDeleteQueryPlan;
        private boolean deleteAllowed;

//        private String nameInSource;
        private String alias;
        private transient Collection keys;
        // order list of elements
        private transient List elements;
        private boolean sup_UPDATE;
        private boolean isVirtualDocument;
        private Collection xmlSchemas;

/**
 * Call constructor to instantiate a runtime object by passing the RuntimeID that identifies the entity and the VIrtualDatabaseID that identifes the Virtual Database the object will be contained.
 */
    public BasicGroup(BasicGroupID groupID, BasicVirtualDatabaseID virtualDBID) {
        super(groupID, virtualDBID);
        keys = new HashSet();
        elements = new ArrayList();
    }

    public String getPath() {
        if(path != null)
	        return path;
        return getID().getPath();
    }
    public String getDescription() {
	      return this.description;
    }
    public String getAlias(){
        return alias;
    }
    public void setAlias(String alias){
        this.alias = alias;
        final MetadataID id = super.getID();
        if (id != null && id instanceof BasicGroupID) {
            ((BasicGroupID)id).setAlias(alias);
        }
    }
    public boolean isPhysical() {
	      return isPhysical;
    }
    public short getTableType() {
	      return tableType;
    }

    public Collection getKeyIDs() {
	      return keys;
    }

    public List getElementIDs() {
	      return elements;
    }
/**
 * @return boolean true indicates this is a RuntimeMetadata System table
 */
    public boolean isSystemTable() {
        if (this.isSystem) {
            return true;
        }
        return false;
    }

    public boolean hasQueryPlan() {
        return hasQueryPlan;
    }

    public String getQueryPlan() {
	      return queryPlan;
    }
	
	public String getMappingDocument() {
	      return queryPlan;
    }
    
    public boolean supportsUpdate() {
        return sup_UPDATE;
    }

	public boolean isVirtualDocument(){        
		return isVirtualDocument;	
	}
	
	public Collection getXMLSchemas(){		
		if(xmlSchemas == null){
			xmlSchemas = Collections.EMPTY_SET;
		}	
		return xmlSchemas;
	}
	
    public void setPath(String path) {
	      this.path = path;
    }
    public void setDescription(String description) {
	      this.description = description;
    }
    public void setIsPhysical(boolean isPhysical) {
        this.isPhysical = isPhysical;
    }
    public void setIsSystem(boolean isSystem) {
        this.isSystem = isSystem;
    }
    public void setQueryPlan(String plan) {
	      this.queryPlan = plan;
    }
    public void setTableType(short type) {
	      this.tableType = type;
          
        if (tableType == MetadataConstants.TABLE_TYPES.TABLE_TYPE ||
            tableType == MetadataConstants.TABLE_TYPES.VIEW_TYPE ||
            tableType == MetadataConstants.TABLE_TYPES.MATERIALIZED_TYPE) {
            
            this.isVirtualDocument = false;              
        } else {
            this.isVirtualDocument = true; 
        }             
          
    }
    public void setKeyIDs(Collection keys) {
	      this.keys = keys;
    }
    public void clearKeyIDs() {
        if(this.keys != null) {
            this.keys.clear();
        }
        //this.keys = null;
    }
    public void setElementIDs(List elements){
	      this.elements = elements;
    }
    public void clearElementIDs() {
        if(this.elements != null) {
            this.elements.clear();
        }
        //this.elements = null;
    }
    public void addKeyID(KeyID keyID) {
        if(keys == null) {
            keys = new HashSet();
        }
        this.keys.add(keyID);
    }
    
    public boolean containsKeyID(KeyID keyID) {
        if (keys == null) {
            return false;
        }
        return this.keys.contains(keyID);
    }
    
    public void removeKeyID(KeyID keyID) {
        if(this.keys != null) {
            this.keys.remove(keyID);
        }        
    }
    
    public void addElementID(ElementID elementID){
        if(elements == null) {
            elements = new ArrayList();
        }
        if ( !this.elements.contains(elementID)) {
	       this.elements.add(elementID);
        }
    }
    
    public void addElementID(int position, ElementID elementID){
        if(elements == null) {
            elements = new ArrayList();
        }
        if ( !this.elements.contains(elementID) && position > 0 ) {
//System.out.println("Adding "+elementID+" under group "+this.getName()+" with position "+position);
            this.elements.add(position-1,elementID);
        } else if ( this.elements.contains(elementID) && position > 0 ) {
            this.elements.set(position-1,elementID);
        }
    }
    
    public boolean containsElementID(ElementID elementID) {
        if (elements == null) {
            return false;
        }
        return this.elements.contains(elementID);
    }
    
    public void removeElementID(ElementID elementID){
        if(elements != null) {
            this.elements.remove(elementID);
        }
    }
    
    public void setHasQueryPlan(boolean hasQueryPlan) {
	      this.hasQueryPlan = hasQueryPlan;
    }
    public void setMappingDocument(String document) {
	      this.queryPlan = document;
    }
    public  void setSupportsUpdate(boolean sup_UPDATE) {
        this.sup_UPDATE = sup_UPDATE;
    }
//    public void setIsVirtualDocument(boolean isVirtualDocument){
//    	this.isVirtualDocument = isVirtualDocument;	
//    }
    public void setXMLSchemas(Collection xmlSchemas){
    	this.xmlSchemas = xmlSchemas;
    }
    /**
     * Returns the deleteQueryPlan.
     * @return String
     */
    public String getDeleteQueryPlan() {
        return this.deleteQueryPlan;
    }

    /**
     * Returns the insertQueryPlan.
     * @return String
     */
    public String getInsertQueryPlan() {
        return this.insertQueryPlan;
    }

    /**
     * Returns the updateQueryPlan.
     * @return String
     */
    public String getUpdateQueryPlan() {
        return this.updateQueryPlan;
    }

    /**
     * Sets the deleteQueryPlan.
     * @param deleteQueryPlan The deleteQueryPlan to set
     */
    public void setDeleteQueryPlan(String deleteQueryPlan) {
        this.deleteQueryPlan = deleteQueryPlan;
    }

    /**
     * Sets the insertQueryPlan.
     * @param insertQueryPlan The insertQueryPlan to set
     */
    public void setInsertQueryPlan(String insertQueryPlan) {
        this.insertQueryPlan = insertQueryPlan;
    }

    /**
     * Sets the updateQueryPlan.
     * @param updateQueryPlan The updateQueryPlan to set
     */
    public void setUpdateQueryPlan(String updateQueryPlan) {
        this.updateQueryPlan = updateQueryPlan;
    }

    /**
     * Sets the hasDeleteQueryPlan.
     * @param hasDeleteQueryPlan The hasDeleteQueryPlan to set
     */
    public void setHasDeleteQueryPlan(boolean hasDeleteQueryPlan) {
        this.hasDeleteQueryPlan = hasDeleteQueryPlan;
    }
    public boolean hasDeleteQueryPlan() {
        return this.hasDeleteQueryPlan;
    }

    /**
     * Sets the hasInsertQueryPlan.
     * @param hasInsertQueryPlan The hasInsertQueryPlan to set
     */
    public void setHasInsertQueryPlan(boolean hasInsertQueryPlan) {
        this.hasInsertQueryPlan = hasInsertQueryPlan;
    }
    public boolean hasInsertQueryPlan() {
        return this.hasInsertQueryPlan;
    }

    /**
     * Sets the hasUpdateQueryPlan.
     * @param hasUpdateQueryPlan The hasUpdateQueryPlan to set
     */
    public void setHasUpdateQueryPlan(boolean hasUpdateQueryPlan) {
        this.hasUpdateQueryPlan = hasUpdateQueryPlan;
    }
    public boolean hasUpdateQueryPlan() {
        return this.hasUpdateQueryPlan;
    }

    /**
     * Returns the deleteAllowed.
     * @return boolean
     */
    public boolean isDeleteAllowed() {
        return deleteAllowed;
    }

    /**
     * Returns the insertAllowed.
     * @return boolean
     */
    public boolean isInsertAllowed() {
        return insertAllowed;
    }

    /**
     * Returns the updateAllowed.
     * @return boolean
     */
    public boolean isUpdateAllowed() {
        return updateAllowed;
    }

    /**
     * Sets the deleteAllowed.
     * @param deleteAllowed The deleteAllowed to set
     */
    public void setDeleteAllowed(boolean deleteAllowed) {
        this.deleteAllowed = deleteAllowed;
    }

    /**
     * Sets the insertAllowed.
     * @param insertAllowed The insertAllowed to set
     */
    public void setInsertAllowed(boolean insertAllowed) {
        this.insertAllowed = insertAllowed;
    }

    /**
     * Sets the updateAllowed.
     * @param updateAllowed The updateAllowed to set
     */
    public void setUpdateAllowed(boolean updateAllowed) {
        this.updateAllowed = updateAllowed;
    }

    public String toString() {
        StringBuffer sw = new StringBuffer();
        
        sw.append("Group: " + this.getName());//$NON-NLS-1$
        sw.append("\n\tSupportsUpdates: " + this.supportsUpdate());//$NON-NLS-1$        
        sw.append("\n\tisVirtual " + this.isVirtualDocument());//$NON-NLS-1$
        sw.append("\n\tisPhysical: " + this.isPhysical());//$NON-NLS-1$
        sw.append("\n\tIsSystem: " + this.isSystemTable());//$NON-NLS-1$

        
        return sw.toString();
        
    }
 }

