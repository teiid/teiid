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

package com.metamatrix.dqp.internal.datamgr.metadata;

import java.util.*;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.StoredProcedureInfo;
import com.metamatrix.query.sql.lang.SPParameter;

/**
 */
public class ProcedureIDImpl extends MetadataIDImpl {

    private StoredProcedureInfo procInfo;
    private MetadataFactory factory;

    /**
     * @param actualMetadataID
     * @param metadata
     * @throws MetaMatrixComponentException
     */
    public ProcedureIDImpl(Object actualMetadataID, StoredProcedureInfo procInfo, MetadataFactory factory, QueryMetadataInterface metadata)
        throws MetaMatrixComponentException {
            
        super(actualMetadataID, metadata);
        this.procInfo = procInfo;
        this.factory = factory;            
    }

    StoredProcedureInfo getProcedureInfo() {
        return this.procInfo;
    }

    /* 
     * @see com.metamatrix.data.metadata.runtime.MetadataID#getChildIDs()
     */
    public List getChildIDs() throws ConnectorException {
        try {
            List parameters = procInfo.getParameters(); 
            List childIDs = new ArrayList(parameters.size());
            Iterator iter = parameters.iterator();
            while(iter.hasNext()){
                SPParameter param = (SPParameter) iter.next();
                if(param.getParameterType() == ParameterInfo.RESULT_SET) {
                    childIDs.add(factory.createResultSetID(this, param.getMetadataID(), param.getResultSetIDs()));    
                } else {
                    childIDs.add(factory.createParameterID(this, param.getMetadataID()));
                }
            }
            return childIDs;
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);
        }            
    }

}
