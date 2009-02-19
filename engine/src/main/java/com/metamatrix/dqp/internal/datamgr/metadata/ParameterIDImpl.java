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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.metadata.runtime.MetadataID;

/**
 */
public class ParameterIDImpl extends MetadataIDImpl {

    private ProcedureIDImpl procID;
    private List resultSetColumns;

    public ParameterIDImpl(Object actualMetadataID, ProcedureIDImpl procedureID, RuntimeMetadataImpl metadata)
        throws MetaMatrixComponentException {
        super(actualMetadataID, metadata);
        
        this.procID = procedureID;
    }

    public ParameterIDImpl(Object actualMetadataID, ProcedureIDImpl procedureID, RuntimeMetadataImpl metadata, List resultSetColumns)
        throws MetaMatrixComponentException {
        super(actualMetadataID, metadata);
        
        this.procID = procedureID;
        this.resultSetColumns = resultSetColumns;
    }

    public MetadataID getParentID() {
        return procID;
    }
    
    public boolean isResultSet() {
        return (this.resultSetColumns != null);
    }
    
    public List getChildIDs() throws ConnectorException {
        if(resultSetColumns != null && resultSetColumns.size() > 0) {
            try {
                List childIDs = new ArrayList(resultSetColumns.size());
                Iterator iter = resultSetColumns.iterator();
                while(iter.hasNext()){
                    Object colID = iter.next();
                    MetadataIDImpl id = new MetadataIDImpl(colID, getMetadata());
                    id.setType(Type.TYPE_ELEMENT);
                    childIDs.add(id);
                }
                return childIDs;
            } catch(MetaMatrixComponentException e) {
                throw new ConnectorException(e);
            }
        }
        return Collections.EMPTY_LIST;
    }
}
