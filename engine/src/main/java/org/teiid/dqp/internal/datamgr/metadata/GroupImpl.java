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

import java.util.ArrayList;
import java.util.List;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.Group;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;

/**
 */
public class GroupImpl extends MetadataObjectImpl implements Group {
	
    GroupImpl(Object actualID, RuntimeMetadataImpl factory){
        super(actualID, factory);
    }
    
    @Override
    public List<Element> getChildren() throws ConnectorException {
    	try {
	    	List elementIds = getMetadata().getElementIDsInGroupID(getActualID());
	    	List<Element> result = new ArrayList<Element>(elementIds.size());
	    	for (Object elementId : elementIds) {
				result.add(new ElementImpl(elementId, getFactory()));
	    	}
	    	return result;
    	} catch (QueryMetadataException e) {
    		throw new ConnectorException(e);
    	} catch (MetaMatrixComponentException e) {
    		throw new ConnectorException(e);
		}
    }
}
