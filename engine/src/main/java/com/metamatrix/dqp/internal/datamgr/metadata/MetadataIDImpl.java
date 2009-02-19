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

/*
 */
package com.metamatrix.dqp.internal.datamgr.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.connector.metadata.runtime.MetadataObject;

/**
 */
public class MetadataIDImpl implements MetadataID {
    private Type type;
    private Object actualMetadataID;
    private String fullName;
    private RuntimeMetadataImpl metadata;
    
    MetadataIDImpl(Object actualMetadataID, RuntimeMetadataImpl metadata) throws MetaMatrixComponentException {
        this.actualMetadataID = actualMetadataID;
        this.metadata = metadata;
        
        if (actualMetadataID != null) {
	        try {
	            fullName = metadata.getMetadata().getFullName(actualMetadataID);
	        } catch (QueryMetadataException ex) {
	            throw new MetaMatrixComponentException(ex, ex.getMessage());
	        }
        }
    }
    
    RuntimeMetadataImpl getMetadata() {
        return this.metadata;
    }
    
    public Type getType() {
        return type;
    }

    public List getChildIDs() throws ConnectorException {
        if(type == Type.TYPE_GROUP && actualMetadataID != null){
            try {
                List children = metadata.getMetadata().getElementIDsInGroupID(actualMetadataID);
                List childIDs = new ArrayList(children.size());
                Iterator iter = children.iterator();
                while(iter.hasNext()){
                    MetadataIDImpl id = new MetadataIDImpl(iter.next(), metadata);
                    id.setType(Type.TYPE_ELEMENT);
                    childIDs.add(id);
                }
                return childIDs;
            } catch(QueryMetadataException e) {
                throw new ConnectorException(e);
            } catch(MetaMatrixComponentException e) {
                throw new ConnectorException(e);
            }
        }
        return Collections.EMPTY_LIST;
    }
                                                  
    public MetadataID getParentID() throws ConnectorException {
        if(type == Type.TYPE_ELEMENT && actualMetadataID != null) {
            try {
                MetadataIDImpl id = new MetadataIDImpl(metadata.getMetadata().getGroupIDForElementID(actualMetadataID), metadata);
                id.setType(Type.TYPE_GROUP);
                return id;
            } catch(QueryMetadataException e) {
                throw new ConnectorException(e);
            } catch(MetaMatrixComponentException e) {
                throw new ConnectorException(e);
            }      
            
        }
        return null;
    }    
    
    public boolean equals(Object obj){
        if (this == obj) {
            return true;
        }

        if (this.getClass().isInstance(obj)) {
            MetadataIDImpl that = (MetadataIDImpl)obj;
            return this.actualMetadataID.equals(that.actualMetadataID);
        }
        
        return false;        
    }
    
    public int hashCode(){
        return actualMetadataID.hashCode();
    }   

    void setType(Type type){
        this.type = type;
    }
    
    Object getActualMetadataID(){
        return this.actualMetadataID;
    }

    public String getFullName() {
        return this.fullName;
    }
    
    /**
     * Get shortName from the metadataID.
     * @return string of shortName
     */    
    public String getName() {
        String shortName = null;
        String fullName = getFullName();
        if (fullName != null && fullName.trim().length() != 0) {
            int index = fullName.lastIndexOf("."); //$NON-NLS-1$
            if(index != -1){            
                shortName = fullName.substring(index + 1);
            }else{
                shortName = fullName;
            }
        }
        return shortName;
    }
    
    @Override
    public MetadataObject getMetadataObject() throws ConnectorException {
    	return this.metadata.getObject(this);
    }
}
