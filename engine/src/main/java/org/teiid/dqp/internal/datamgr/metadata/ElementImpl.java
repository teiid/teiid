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
package org.teiid.dqp.internal.datamgr.metadata;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.Group;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.metadata.SupportConstants;

/**
 */
public class ElementImpl extends TypeModelImpl implements Element {
    
    ElementImpl(Object actualID, RuntimeMetadataImpl factory){
        super(actualID, factory);
    }
    
    public Class getJavaType() throws ConnectorException {
        try {
            String elementType = getMetadata().getElementType(getActualID());
            return DataTypeManager.getDataTypeClass(elementType);
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }
    
    public int getPosition() throws ConnectorException {
        try {
            return getMetadata().getPosition(getActualID()) - 1;
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }

    public Object getMaximumValue() throws ConnectorException {
        try {
            return getMetadata().getMaximumValue(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }

    public Object getMinimumValue() throws ConnectorException {
        try {
            return getMetadata().getMinimumValue(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }

    public int getSearchability() throws ConnectorException {
        try {
            boolean comparable = getMetadata().elementSupports(getActualID(), SupportConstants.Element.SEARCHABLE_COMPARE);
            boolean likable = getMetadata().elementSupports(getActualID(), SupportConstants.Element.SEARCHABLE_LIKE);
            if(comparable) {
                if(likable) {
                    return Element.SEARCHABLE;
                }
                return Element.SEARCHABLE_COMPARE;
            }
            if(likable) {
                return Element.SEARCHABLE_LIKE;
            }
            return Element.NOT_SEARCHABLE;                    
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }

    public boolean isAutoIncremented() throws ConnectorException {
        try {
            return getMetadata().elementSupports(getActualID(), SupportConstants.Element.AUTO_INCREMENT);
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }

    public boolean isCaseSensitive() throws ConnectorException {
        try {
            return getMetadata().elementSupports(getActualID(), SupportConstants.Element.CASE_SENSITIVE);
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }

    public String getNativeType() throws ConnectorException {
        try {
            return getMetadata().getNativeType(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
    }

	@Override
	public String getFormat() throws ConnectorException {
        try {
            return getMetadata().getFormat(getActualID());
        } catch(QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch(MetaMatrixComponentException e) {
            throw new ConnectorException(e);            
        }
	}

	@Override
	public Group getParent() throws ConnectorException {
		Object groupId;
		try {
			groupId = this.getMetadata().getGroupIDForElementID(getActualID());
		} catch (QueryMetadataException e) {
			throw new ConnectorException(e);
		} catch (MetaMatrixComponentException e) {
			throw new ConnectorException(e);
		}
		return new GroupImpl(groupId, getFactory());
	}  
}
