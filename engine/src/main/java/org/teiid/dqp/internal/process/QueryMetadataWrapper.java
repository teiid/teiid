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

package org.teiid.dqp.internal.process;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.BasicQueryMetadataWrapper;

/**
 * This classs is a proxy to QueryMetadataInterface. It knows VDBService
 * and VNB name.
 */
public class QueryMetadataWrapper extends BasicQueryMetadataWrapper{
	private String vdbName;
	private String vdbVersion;
	private VDBService vdbService;
	
	public QueryMetadataWrapper(QueryMetadataInterface actualMetadata, String vdbName, String vdbVersion, VDBService vdbService){
		super(actualMetadata);
		this.vdbName = vdbName;
		this.vdbService = vdbService;
		this.vdbVersion = vdbVersion;
	}
	
    protected QueryMetadataInterface getActualMetadata() {
        return this.actualMetadata;
    }
    protected String getVdbName() {
        return this.vdbName;
    }
    protected VDBService getVdbService() {
        return this.vdbService;
    }
    protected String getVdbVersion() {
        return this.vdbVersion;
    }
    
	/**
	 * @see com.metamatrix.query.metadata.QueryMetadataInterface#getGroupsForPartialName(java.lang.String)
	 */
	public Collection getGroupsForPartialName(String partialGroupName) throws MetaMatrixComponentException, QueryMetadataException {
		Collection result = actualMetadata.getGroupsForPartialName(partialGroupName);
		if(result == null || result.isEmpty()){
			return result;
		}
		Collection filteredResult = new HashSet();
		Iterator iter = result.iterator();
		while(iter.hasNext()){
			String groupName = (String)iter.next();
	        Object groupID = actualMetadata.getGroupID(groupName);
	        Object modelID = actualMetadata.getModelID(groupID);
	        String modelName = actualMetadata.getFullName(modelID);
	        int visibility = vdbService.getModelVisibility(vdbName, vdbVersion, modelName);
	        if(visibility == ModelInfo.PUBLIC){
	        	filteredResult.add(groupName);
	        }
		}
		return filteredResult;
	}

}
