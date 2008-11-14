/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.*;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.query.metadata.QueryMetadataInterface;

/**
 */
public class ParameterIDImpl extends MetadataIDImpl {

    private ProcedureIDImpl procID;
    private List resultSetColumns;

    public ParameterIDImpl(Object actualMetadataID, ProcedureIDImpl procedureID, QueryMetadataInterface metadata)
        throws MetaMatrixComponentException {
        super(actualMetadataID, metadata);
        
        this.procID = procedureID;
    }

    public ParameterIDImpl(Object actualMetadataID, ProcedureIDImpl procedureID, QueryMetadataInterface metadata, List resultSetColumns)
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
                    id.setType(MetadataID.TYPE_ELEMENT);
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
