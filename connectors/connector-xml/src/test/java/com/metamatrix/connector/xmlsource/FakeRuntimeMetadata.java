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

package com.metamatrix.connector.xmlsource;

import java.util.Properties;

import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.connector.metadata.runtime.MetadataObject;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

public class FakeRuntimeMetadata implements RuntimeMetadata {
    String fileName;
    
    public FakeRuntimeMetadata(String fileName) {
        this.fileName = fileName; 
    }
    
    public MetadataObject getObject(MetadataID id) throws ConnectorException {
        return new MetadataObject() {
            public MetadataID getMetadataID() {
                return null;
            }
            public String getNameInSource() throws ConnectorException {
                return fileName; 
            }
            public Properties getProperties() throws ConnectorException {
                return null;
            }                        
        };
    }
    public byte[] getBinaryVDBResource(String resourcePath) throws ConnectorException {
        return null;
    }
    public String getCharacterVDBResource(String resourcePath) throws ConnectorException {
        return null;
    }
    public String[] getVDBResourcePaths() throws ConnectorException {
        return null;
    }                
}