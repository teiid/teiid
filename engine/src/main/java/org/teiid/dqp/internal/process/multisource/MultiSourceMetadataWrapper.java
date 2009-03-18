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

package org.teiid.dqp.internal.process.multisource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.metadata.BasicQueryMetadataWrapper;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;

/**
 * This classs is a proxy to QueryMetadataInterface. It knows VDBService
 * and VNB name.
 */
public class MultiSourceMetadataWrapper extends BasicQueryMetadataWrapper {
	
    private Collection multiSourceModels;
    
    public MultiSourceMetadataWrapper(QueryMetadataInterface actualMetadata, Collection multiSourceModels){
    	super(actualMetadata);
        this.multiSourceModels = multiSourceModels;
    }	

    /**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementID(java.lang.String)
	 */
	public Object getElementID(String elementName) throws MetaMatrixComponentException, QueryMetadataException {
        String shortName = getShortElementName(elementName);        
        if(shortName.equalsIgnoreCase(MultiSourceElement.MULTI_SOURCE_ELEMENT_NAME)) {
            try {
                String groupName = getGroupName(elementName);
                Object groupID = getGroupID(groupName);
                List elements = getElementIDsInGroupID(groupID);
                
                Iterator iter = elements.iterator();
                while(iter.hasNext()) {
                    Object id = iter.next();
                    if(id instanceof MultiSourceElement) {
                        return id;
                    }
                }
                
            } catch(Exception e) {
                // ignore and try the real metadata below
            }
        }
        
		return actualMetadata.getElementID(elementName);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getModelID(java.lang.Object)
	 */
	public Object getModelID(Object groupOrElementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(groupOrElementID instanceof MultiSourceElement) {
            Object groupID = ((MultiSourceElement)groupOrElementID).groupID;
            return this.getModelID(groupID);
        } 
        
        return actualMetadata.getModelID(groupOrElementID);            
    }

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getFullName(java.lang.Object)
	 */
	public String getFullName(Object metadataID) throws MetaMatrixComponentException, QueryMetadataException {
        if(metadataID instanceof MultiSourceElement) {
            return ((MultiSourceElement)metadataID).fullName;
        }
		return actualMetadata.getFullName(metadataID);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementIDsInGroupID(java.lang.Object)
	 */
	public List getElementIDsInGroupID(Object groupID) throws MetaMatrixComponentException, QueryMetadataException {
        List elements = actualMetadata.getElementIDsInGroupID(groupID);
        
        Object modelID = this.getModelID(groupID);
        String modelName = this.getFullName(modelID);
        if(multiSourceModels.contains(modelName)) {
            elements = new ArrayList(elements);       
            
            String fullName = this.getFullName(groupID) + "." + MultiSourceElement.MULTI_SOURCE_ELEMENT_NAME; //$NON-NLS-1$

            // Check whether a source_name column was modeled in the group already
            boolean elementExists = false;
            for(int i=0; i<elements.size(); i++) {
                Object elemID = elements.get(i);
                if(actualMetadata.getFullName(elemID).endsWith("." + MultiSourceElement.MULTI_SOURCE_ELEMENT_NAME)) { //$NON-NLS-1$
                    // Replace the element with a MultiSourceElement
                    elements.set(i, new MultiSourceElement(groupID, i+1, fullName));
                    elementExists = true;
                    break;
                }
            }
            
            // Append a new pseudo-column to the end
            if(!elementExists) {
                int position = elements.size()+1;
                elements.add(new MultiSourceElement(groupID, position, fullName));
            }
        }

		return elements;
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupIDForElementID(java.lang.Object)
	 */
	public Object getGroupIDForElementID(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return ((MultiSourceElement)elementID).groupID;
        } 
        
		return actualMetadata.getGroupIDForElementID(elementID);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementType(java.lang.Object)
	 */
	public String getElementType(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return DataTypeManager.DefaultDataTypes.STRING;
        } 
        
		return actualMetadata.getElementType(elementID);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getDefaultValue(java.lang.Object)
	 */
	public Object getDefaultValue(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return null;
        } 
        
		return actualMetadata.getDefaultValue(elementID);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMinimumValue(java.lang.Object)
	 */
	public Object getMinimumValue(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return null;
        } 
        
		return actualMetadata.getMinimumValue(elementID);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getMaximumValue(java.lang.Object)
	 */
	public Object getMaximumValue(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return null;
        } 
        
		return actualMetadata.getMaximumValue(elementID);
	}

    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getDistinctValues(java.lang.Object)
     */
    public int getDistinctValues(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return ((MultiSourceElement)elementID).position;
        } 
        
        return actualMetadata.getDistinctValues(elementID);
    }

    /**
     * @see com.metamatrix.query.metadata.QueryMetadataInterface#getNullValues(java.lang.Object)
     */
    public int getNullValues(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return ((MultiSourceElement)elementID).position;
        } 
        
        return actualMetadata.getNullValues(elementID);
    }

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getPosition(java.lang.Object)
	 */
	public int getPosition(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return ((MultiSourceElement)elementID).position;
        } 
        
		return actualMetadata.getPosition(elementID);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getPrecision(java.lang.Object)
	 */
	public int getPrecision(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return 0;
        } 
        
		return actualMetadata.getPrecision(elementID);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getScale(java.lang.Object)
	 */
	public int getScale(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return 0;
        } 
        
		return actualMetadata.getScale(elementID);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getRadix(java.lang.Object)
	 */
	public int getRadix(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return 0;
        } 
        
		return actualMetadata.getRadix(elementID);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#elementSupports(java.lang.Object, int)
	 */
	public boolean elementSupports(Object elementID, int elementConstant) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            switch(elementConstant) {
                case SupportConstants.Element.NULL:
                    return false;
                case SupportConstants.Element.NULL_UNKNOWN:
                    return false;
                case SupportConstants.Element.SEARCHABLE_COMPARE:
                    return true;
                case SupportConstants.Element.SEARCHABLE_LIKE:
                    return true;
                case SupportConstants.Element.SELECT:
                    return true;
                case SupportConstants.Element.UPDATE:
                    return false;
                case SupportConstants.Element.DEFAULT_VALUE:
                    return false;
                case SupportConstants.Element.AUTO_INCREMENT:
                    return false;
                case SupportConstants.Element.CASE_SENSITIVE:
                    return false;
                case SupportConstants.Element.SIGNED:
                    return false;
                default:
                    throw new UnsupportedOperationException("Attempt to check support for unknown constant: " + elementConstant); //$NON-NLS-1$
            }
        } 
        
		return actualMetadata.elementSupports(elementID, elementConstant);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getExtensionProperties(java.lang.Object)
	 */
	public Properties getExtensionProperties(Object metadataID) throws MetaMatrixComponentException, QueryMetadataException {
        if(metadataID instanceof MultiSourceElement) {
            return new Properties();
        }
		return actualMetadata.getExtensionProperties(metadataID);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getNameInSource(java.lang.Object)
	 */
	public String getNameInSource(Object metadataID) throws MetaMatrixComponentException, QueryMetadataException {
        if(metadataID instanceof MultiSourceElement) {
            return null;
        } 
        
		return actualMetadata.getNameInSource(metadataID);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getElementLength(java.lang.Object)
	 */
	public int getElementLength(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return 255;
        } 
        
		return actualMetadata.getElementLength(elementID);
	}

	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getNativeType(java.lang.Object)
	 */
	public String getNativeType(Object elementID) throws MetaMatrixComponentException, QueryMetadataException {
        if(elementID instanceof MultiSourceElement) {
            return null;
        } 
        
		return actualMetadata.getNativeType(elementID);
	}

}
