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
package org.teiid.dqp.internal.datamgr;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.ArgCheck;
import org.teiid.metadata.Column;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.*;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.translator.TranslatorException;


/**
 */
public class RuntimeMetadataImpl implements RuntimeMetadata {
    private QueryMetadataInterface metadata;
    
    public RuntimeMetadataImpl(QueryMetadataInterface metadata){
    	ArgCheck.isNotNull(metadata);
        this.metadata = metadata;
    }
    
    @Override
    public Column getColumn(String fullName) throws TranslatorException {
		try {
			Object metadataId = metadata.getElementID(fullName);
			return getElement(metadataId);
		} catch (QueryMetadataException e) {
			throw new TranslatorException(e);
		} catch (TeiidComponentException e) {
			throw new TranslatorException(e);
		}
    }
    
    public Column getElement(Object elementId) {
    	if (elementId instanceof Column) {
    		return (Column)elementId;
    	}
    	return null;
    }
    
    @Override
    public Table getTable(String fullName) throws TranslatorException {
		try {
			Object groupId = metadata.getGroupID(fullName);
	    	return getGroup(groupId);
		} catch (QueryMetadataException e) {
			throw new TranslatorException(e);
		} catch (TeiidComponentException e) {
			throw new TranslatorException(e);
		}
    }

	public Table getGroup(Object groupId) throws QueryMetadataException, TeiidComponentException {
		if (groupId instanceof Table && !metadata.isVirtualGroup(groupId)) {
			return (Table)groupId;
		}
		return null;
	}    
    
    @Override
    public Procedure getProcedure(String fullName) throws TranslatorException {
		try {
			StoredProcedureInfo sp = metadata.getStoredProcedureInfoForProcedure(fullName);
	    	return getProcedure(sp);
		} catch (QueryMetadataException e) {
			throw new TranslatorException(e);
		} catch (TeiidComponentException e) {
			throw new TranslatorException(e);
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
    
    public byte[] getBinaryVDBResource(String resourcePath) throws TranslatorException {
        try {
            return metadata.getBinaryVDBResource(resourcePath);
        } catch (QueryMetadataException e) {
            throw new TranslatorException(e);
        } catch (TeiidComponentException e) {
            throw new TranslatorException(e);
        }
    }

    public String getCharacterVDBResource(String resourcePath) throws TranslatorException {
        try {
            return metadata.getCharacterVDBResource(resourcePath);
        } catch (QueryMetadataException e) {
            throw new TranslatorException(e);
        } catch (TeiidComponentException e) {
            throw new TranslatorException(e);
        }
    }

    public String[] getVDBResourcePaths() throws TranslatorException {
        try {
            return metadata.getVDBResourcePaths();
        } catch (QueryMetadataException e) {
            throw new TranslatorException(e);
        } catch (TeiidComponentException e) {
            throw new TranslatorException(e);
        }
    }
    
    public QueryMetadataInterface getMetadata() {
    	return metadata;
    }
    
}
