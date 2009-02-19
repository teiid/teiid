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

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.metadata.runtime.*;
import com.metamatrix.connector.metadata.runtime.MetadataID.Type;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.StoredProcedureInfo;

/**
 */
public class MetadataFactory {
    private QueryMetadataInterface metadata;
    private RuntimeMetadataImpl runtimeMetadata;
       
    public MetadataFactory (QueryMetadataInterface metadata){
        ArgCheck.isNotNull(metadata);
        this.metadata = metadata;
        this.runtimeMetadata = new RuntimeMetadataImpl(this);
    }
    
    public RuntimeMetadataImpl getRuntimeMetadata(){
        return this.runtimeMetadata;
    }
    
    /**
     * Method for creating group and element MetadataIDs. It will not return an id for virtual groups.
     * @param metadataID Object from QueryMetadataInterface representing group or element identifier
     * @param type Either TYPE_ELEMENT or TYPE_GROUP
     * @return MetadataID 
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    public MetadataID createMetadataID(Object metadataID, Type type) throws QueryMetadataException, MetaMatrixComponentException{
    	if (type == Type.TYPE_GROUP && metadata.isVirtualGroup(metadataID)) {
    		return null;
    	}
        MetadataIDImpl id = new MetadataIDImpl(metadataID, getRuntimeMetadata());
        id.setType(type);
        return id;
    }

    /**
     * Method for creating procedure MetadataIDs.  
     * @param procedureID Procedure ID
     * @return Parameter ID
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    public MetadataID createProcedureID(Object metadataID) throws QueryMetadataException, MetaMatrixComponentException{
        String procName = metadata.getFullName(metadataID);
        StoredProcedureInfo info = metadata.getStoredProcedureInfoForProcedure(procName);        
        MetadataIDImpl id = new ProcedureIDImpl(metadataID, info, this);
        id.setType(Type.TYPE_PROCEDURE);
        return id;
    }
    
    /**
     * Special factory method for creating parameter MetadataIDs based on a procedure MetadataID.  
     * @param procedureID Procedure ID
     * @return Parameter ID
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    public MetadataID createParameterID(ProcedureIDImpl procedureID, Object metadataID) throws QueryMetadataException, MetaMatrixComponentException{
        ParameterIDImpl id = new ParameterIDImpl(metadataID, procedureID, getRuntimeMetadata());
        id.setType(Type.TYPE_PARAMETER);
        return id;
    }

    /**
     * Special factory method for creating result set MetadataIDs based on a procedure MetadataID.  
     * @param procedureID Procedure ID
     * @return Parameter ID
     * @throws QueryMetadataException
     * @throws MetaMatrixComponentException
     */
    public MetadataID createResultSetID(ProcedureIDImpl procedureID, Object metadataID, List resultSetColumns) throws QueryMetadataException, MetaMatrixComponentException{
        ParameterIDImpl id = new ParameterIDImpl(metadataID, procedureID, getRuntimeMetadata(), resultSetColumns);
        id.setType(Type.TYPE_PARAMETER);
        return id;
    }
    
    public MetadataObject createMetadataObject(MetadataID id) throws QueryMetadataException, MetaMatrixComponentException, ConnectorException {
    	Type type = id.getType();
        MetadataIDImpl idImpl = (MetadataIDImpl) id;
        MetadataObject mObj = null;
        if(type == Type.TYPE_GROUP){
            mObj = new GroupImpl(idImpl);
            
        } else if(type == Type.TYPE_ELEMENT){            
            mObj = new ElementImpl(idImpl);
            
        } else if(type == Type.TYPE_PROCEDURE){
            mObj = new ProcedureImpl(idImpl);
            
        } else if(type == Type.TYPE_PARAMETER) {
            mObj = new ParameterImpl(idImpl);                
            
        } else{
            //throw exception
        }
        return mObj;
    }
    
    public byte[] getBinaryVDBResource(String resourcePath) throws MetaMatrixComponentException, QueryMetadataException {
        return metadata.getBinaryVDBResource(resourcePath);
    }

    public String getCharacterVDBResource(String resourcePath) throws MetaMatrixComponentException, QueryMetadataException {
        return metadata.getCharacterVDBResource(resourcePath);
    }

    public String[] getVDBResourcePaths() throws MetaMatrixComponentException, QueryMetadataException {
        return metadata.getVDBResourcePaths();
    }
    
    public QueryMetadataInterface getMetadata() {
		return metadata;
	}
}
