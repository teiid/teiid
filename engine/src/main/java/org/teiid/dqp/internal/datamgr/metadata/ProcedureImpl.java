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

import org.teiid.connector.metadata.runtime.Parameter;
import org.teiid.connector.metadata.runtime.Procedure;

import com.metamatrix.query.metadata.StoredProcedureInfo;
import com.metamatrix.query.sql.lang.SPParameter;

/**
 */
public class ProcedureImpl extends MetadataObjectImpl implements Procedure {
	
	private StoredProcedureInfo procInfo;

    ProcedureImpl(RuntimeMetadataImpl factory, StoredProcedureInfo procInfo){
        super(procInfo.getProcedureID(), factory);
        this.procInfo = procInfo;
    }
    
    @Override
    public List<Parameter> getChildren() {
    	List<Parameter> result = new ArrayList<Parameter>(procInfo.getParameters().size());
    	for (SPParameter param : procInfo.getParameters()) {
    		result.add(new ParameterImpl(getFactory(), param, this));
    	}
    	return result;
    }
    
}
