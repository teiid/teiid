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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.BasicQueryMetadataWrapper;
import org.teiid.query.metadata.QueryMetadataInterface;


/**
 * This classs is a proxy to QueryMetadataInterface. 
 */
public class MultiSourceMetadataWrapper extends BasicQueryMetadataWrapper {
	
	private static class MultiSourceGroup {
		Object multiSourceElement;
		List<?> columns;
	}
	
	private Set<String> multiSourceModels;
	private String multiSourceElementName;
	private Map<Object, MultiSourceGroup> groups = new HashMap<Object, MultiSourceGroup>();
	
	public static String getGroupName(final String fullElementName) {
        int index = fullElementName.lastIndexOf('.');
        if(index >= 0) { 
            return fullElementName.substring(0, index);
        }
        return null;
    }
	
    public MultiSourceMetadataWrapper(final QueryMetadataInterface actualMetadata, Set<String> multiSourceModels, String multiSourceElementName){
    	super(actualMetadata);
        this.multiSourceModels = multiSourceModels;
        this.multiSourceElementName = multiSourceElementName;
    }	

    public MultiSourceMetadataWrapper(QueryMetadataInterface metadata,
    		Set<String> multiSourceModels) {
    	this(metadata, multiSourceModels, MultiSourceElement.DEFAULT_MULTI_SOURCE_ELEMENT_NAME);
	}
    
	@Override
	public List<?> getElementIDsInGroupID(Object groupID)
			throws TeiidComponentException, QueryMetadataException {
		MultiSourceGroup msg = getMultiSourceGroup(groupID);
		if (msg != null) {
			return msg.columns;
		}
		return actualMetadata.getElementIDsInGroupID(groupID);
	}

	public MultiSourceGroup getMultiSourceGroup(Object groupID)
			throws TeiidComponentException, QueryMetadataException {
		MultiSourceGroup msg = groups.get(groupID);
		if (msg != null) {
			return msg;
		}
		List<?> elements = actualMetadata.getElementIDsInGroupID(groupID);
        // Check whether a source_name column was modeled in the group already
        Object mse = null;
        for(int i = 0; i<elements.size() && mse == null; i++) {
            Object elemID = elements.get(i);
            if(actualMetadata.getName(elemID).equalsIgnoreCase(multiSourceElementName)) {
            	if (!actualMetadata.getElementType(elemID).equalsIgnoreCase(DataTypeManager.DefaultDataTypes.STRING)) {
            		throw new QueryMetadataException(QueryPlugin.Event.TEIID31128, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31128, multiSourceElementName, groupID));
            	}
            	mse = elemID;
            }
        }
        
        if (mse == null) {
        	List<Object> result = new ArrayList<Object>(elements);	        	
    		MultiSourceElement e = new MultiSourceElement();
            e.setName(multiSourceElementName);
            e.setParent((Table)groupID);
        	e.setPosition(elements.size()+1);
        	e.setRuntimeType(DataTypeManager.DefaultDataTypes.STRING);
        	e.setNullValues(0);
        	e.setNullType(NullType.No_Nulls);
        	e.setSearchType(SearchType.Searchable);
        	e.setUpdatable(true);
        	e.setLength(255);
    		result.add(e);
    		mse = e;
    		elements = result;
        }
        msg = new MultiSourceGroup();
        msg.columns = elements;
        msg.multiSourceElement = mse;
        this.groups.put(groupID, msg);
        return msg;
	}
	
	@Override
	public Object getElementID(String elementName)
			throws TeiidComponentException, QueryMetadataException {
		if (elementName.length() > multiSourceElementName.length() 
				&& elementName.charAt(elementName.length() - 1 - multiSourceElementName.length()) == '.'
				&& elementName.endsWith(multiSourceElementName) ) {
			String group = getGroupName(elementName);
			if (group != null) {
				MultiSourceGroup msg = getMultiSourceGroup(getGroupID(group));
				if (msg != null) {
					return msg.multiSourceElement;
				}
			}
		}
		return super.getElementID(elementName);
	}
	
	@Override
	public boolean isMultiSource(Object modelId) throws QueryMetadataException, TeiidComponentException {
		return multiSourceModels.contains(getFullName(modelId));
	}
	
	@Override
	public boolean isMultiSourceElement(Object elementId) throws QueryMetadataException, TeiidComponentException {
		String shortName = getName(elementId);        
        if (shortName.equalsIgnoreCase(multiSourceElementName)) {
    		Object gid = getGroupIDForElementID(elementId);
    		Object modelID = this.getModelID(gid);
            String modelName = this.getFullName(modelID);
            if(multiSourceModels.contains(modelName)) {
            	return true;
            }
        }
		return false;
	}
	
	@Override
	protected QueryMetadataInterface createDesignTimeMetadata() {
		return new MultiSourceMetadataWrapper(actualMetadata.getDesignTimeMetadata(), multiSourceModels, multiSourceElementName);
	}
	
	@Override
	public boolean isPseudo(Object elementId) {
		if (elementId instanceof MultiSourceElement) {
			return true;
		}
		return actualMetadata.isPseudo(elementId);
	}

}
