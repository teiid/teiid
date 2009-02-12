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

import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.metadata.runtime.Element;
import com.metamatrix.connector.metadata.runtime.Parameter;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.metadata.StoredProcedureInfo;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.sql.lang.SPParameter;

/**
 */
public class ParameterImpl extends TypeModelImpl implements Parameter {

    private SPParameter param;

    /**
     * @param metadataID
     */
    ParameterImpl(MetadataIDImpl metadataID) {
        super(metadataID);
    }

    private SPParameter getParameterInfo() throws ConnectorException {
        if(param == null) {
            ParameterIDImpl paramID = (ParameterIDImpl) getMetadataID();
            StoredProcedureInfo procInfo = ((ProcedureIDImpl)paramID.getParentID()).getProcedureInfo();        
            List params = procInfo.getParameters();
            for(int i=0; i<params.size(); i++) {
                SPParameter aParam = (SPParameter) params.get(i);
                if(aParam.getMetadataID().equals(paramID.getActualMetadataID())) {
                    this.param = aParam;
                    break;
                }
            }
        }        
        return param;  
    }

    /* 
     * @see com.metamatrix.data.metadata.runtime.Parameter#getIndex()
     */
    public int getIndex() throws ConnectorException {
        SPParameter paramInfo = getParameterInfo();
        return paramInfo.getIndex();
    }

    /* 
     * @see com.metamatrix.data.metadata.runtime.Parameter#getDirection()
     */
    public int getDirection() throws ConnectorException {
        SPParameter paramInfo = getParameterInfo();
        switch(paramInfo.getParameterType()) {
            case ParameterInfo.IN:            return Parameter.IN;
            case ParameterInfo.INOUT:         return Parameter.INOUT;
            case ParameterInfo.OUT:           return Parameter.OUT;
            case ParameterInfo.RESULT_SET:    return Parameter.RESULT_SET;
            case ParameterInfo.RETURN_VALUE:  return Parameter.RETURN;
            default:    
                throw new ConnectorException(DQPPlugin.Util.getString("ParameterImpl.Invalid_direction", paramInfo.getParameterType())); //$NON-NLS-1$
        }        
    }

    /* 
     * @see com.metamatrix.data.metadata.runtime.Parameter#getJavaType()
     */
    public Class getJavaType() throws ConnectorException {
        SPParameter paramInfo = getParameterInfo();
        return paramInfo.getClassType();
    }

    /** 
     * @see com.metamatrix.connector.metadata.runtime.TypeModel#getNullability()
     * @since 4.3
     */
    public int getNullability() throws ConnectorException {
        try {
            boolean allowsNull = getMetadata().elementSupports(getActualID(), SupportConstants.Element.NULL);
            boolean nullUnknown = getMetadata().elementSupports(getActualID(), SupportConstants.Element.NULL_UNKNOWN);
            if(nullUnknown) {
                return Element.NULLABLE_UNKNOWN;
            }
            if(allowsNull) {
                return Element.NULLABLE;
            }
            return Element.NOT_NULLABLE;                    
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }
    
    

}
