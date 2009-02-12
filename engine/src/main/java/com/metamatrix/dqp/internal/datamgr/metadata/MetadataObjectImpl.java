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

import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.connector.metadata.runtime.MetadataObject;
import com.metamatrix.query.metadata.QueryMetadataInterface;

/**
 */
public abstract class MetadataObjectImpl implements MetadataObject {
    private MetadataIDImpl metadataID;
    
    MetadataObjectImpl(MetadataIDImpl metadataID){
        this.metadataID = metadataID;
    }
    
    public MetadataID getMetadataID() {
        return metadataID;
    }
    
    Object getActualID() {
        return metadataID.getActualMetadataID();
    }

    QueryMetadataInterface getMetadata() {
        return metadataID.getMetadata();
    }

    public String getNameInSource() throws ConnectorException {
        try {
            return getMetadata().getNameInSource(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }

    public Properties getProperties() throws ConnectorException {
        try {
            return getMetadata().getExtensionProperties(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }
    
    public boolean equals(Object obj){
        if (this == obj) {
            return true;
        }

        if (this.getClass().isInstance(obj)) {
            MetadataObjectImpl that = (MetadataObjectImpl)obj;
            return this.metadataID.equals(that.metadataID);
        }
        
        return false;        
    }
    
    public int hashCode(){
        return metadataID.hashCode();
    } 

}
