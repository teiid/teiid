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
import org.teiid.connector.metadata.runtime.*;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.StoredProcedureInfo;
import com.metamatrix.query.sql.lang.SPParameter;

/**
 */
public class RuntimeMetadataImpl implements RuntimeMetadata {
    private QueryMetadataInterface metadata;
    
    public RuntimeMetadataImpl(QueryMetadataInterface metadata){
    	ArgCheck.isNotNull(metadata);
        this.metadata = metadata;
    }
    
    @Override
    public Column getElement(String fullName) throws ConnectorException {
		try {
			Object metadataId = metadata.getElementID(fullName);
			return getElement(metadataId);
		} catch (QueryMetadataException e) {
			throw new ConnectorException(e);
		} catch (MetaMatrixComponentException e) {
			throw new ConnectorException(e);
		}
    }
    
    public Column getElement(Object elementId) {
    	if (elementId instanceof Column) {
    		return (Column)elementId;
    	}
    	return null;
    }
    
    @Override
    public Table getGroup(String fullName) throws ConnectorException {
		try {
			Object groupId = metadata.getGroupID(fullName);
	    	return getGroup(groupId);
		} catch (QueryMetadataException e) {
			throw new ConnectorException(e);
		} catch (MetaMatrixComponentException e) {
			throw new ConnectorException(e);
		}
    }

	public Table getGroup(Object groupId) throws QueryMetadataException, MetaMatrixComponentException {
		if (!metadata.isVirtualGroup(groupId) && groupId instanceof Table) {
			return (Table)groupId;
		}
		return null;
	}    
    
    @Override
    public Procedure getProcedure(String fullName) throws ConnectorException {
		try {
			StoredProcedureInfo sp = metadata.getStoredProcedureInfoForProcedure(fullName);
	    	return getProcedure(sp);
		} catch (QueryMetadataException e) {
			throw new ConnectorException(e);
		} catch (MetaMatrixComponentException e) {
			throw new ConnectorException(e);
		}
    }

	public Procedure getProcedure(StoredProcedureInfo sp) {
		if (sp.getProcedureID() instanceof Procedure) {
			return (Procedure)sp.getProcedureID();
		}
		return null;
	}
	
	public ProcedureParameter getParameter(SPParameter param) {
		if (param.getMetadataID() instanceof ProcedureParameter) {
			return (ProcedureParameter)param.getMetadataID();
		}
		return null;
	}
    
    public byte[] getBinaryVDBResource(String resourcePath) throws ConnectorException {
        try {
            return metadata.getBinaryVDBResource(resourcePath);
        } catch (QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch (MetaMatrixComponentException e) {
            throw new ConnectorException(e);
        }
    }

    public String getCharacterVDBResource(String resourcePath) throws ConnectorException {
        try {
            return metadata.getCharacterVDBResource(resourcePath);
        } catch (QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch (MetaMatrixComponentException e) {
            throw new ConnectorException(e);
        }
    }

    public String[] getVDBResourcePaths() throws ConnectorException {
        try {
            return metadata.getVDBResourcePaths();
        } catch (QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch (MetaMatrixComponentException e) {
            throw new ConnectorException(e);
        }
    }
    
    public QueryMetadataInterface getMetadata() {
    	return metadata;
    }
    
}
