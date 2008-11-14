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

/*
 */
package com.metamatrix.dqp.internal.datamgr.metadata;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.*;

/**
 */
public class RuntimeMetadataImpl implements RuntimeMetadata {
    private MetadataFactory factory;
    
    public RuntimeMetadataImpl(MetadataFactory factory){
        this.factory = factory;
    }
    
    public MetadataObject getObject(MetadataID id) throws ConnectorException {

        try {
            return factory.createMetadataObject(id);
        } catch (QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch (MetaMatrixComponentException e) {
            throw new ConnectorException(e);
        }
    }

    public byte[] getBinaryVDBResource(String resourcePath) throws ConnectorException {
        try {
            return factory.getBinaryVDBResource(resourcePath);
        } catch (QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch (MetaMatrixComponentException e) {
            throw new ConnectorException(e);
        }
    }

    public String getCharacterVDBResource(String resourcePath) throws ConnectorException {
        try {
            return factory.getCharacterVDBResource(resourcePath);
        } catch (QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch (MetaMatrixComponentException e) {
            throw new ConnectorException(e);
        }
    }

    public String[] getVDBResourcePaths() throws ConnectorException {
        try {
            return factory.getVDBResourcePaths();
        } catch (QueryMetadataException e) {
            throw new ConnectorException(e);
        } catch (MetaMatrixComponentException e) {
            throw new ConnectorException(e);
        }
    }
}
