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

package org.teiid.dqp.internal.datamgr.metadata;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.TypeModel;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.metadata.SupportConstants;


/** 
 * @since 4.3
 */
public abstract class TypeModelImpl extends MetadataObjectImpl implements
                                                TypeModel {

    /** 
     * @since 4.3
     */
    public TypeModelImpl(Object actualID, RuntimeMetadataImpl factory) {
        super(actualID, factory);
    }

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

    public Object getDefaultValue() throws ConnectorException {
        try {
            return getMetadata().getDefaultValue(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }

    public int getLength() throws ConnectorException {
        try {
            return getMetadata().getElementLength(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }
    
    public int getPrecision() throws ConnectorException {
        try {
            return getMetadata().getPrecision(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }

    public int getScale() throws ConnectorException {
        try {
            return getMetadata().getScale(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }

    public String getModeledType() throws ConnectorException {
        try {
            return getMetadata().getModeledType(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }

    public String getModeledBaseType() throws ConnectorException {
        try {
            return getMetadata().getModeledBaseType(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }
    
    public String getModeledPrimitiveType() throws ConnectorException {
        try {
            return getMetadata().getModeledPrimitiveType(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }
    

}
