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

package com.metamatrix.connector.language;

import com.metamatrix.connector.metadata.runtime.MetadataID;

/**
 * This interface is used to mark language objects as having a 
 * reference to a MetadataID.  
 */
public interface IMetadataReference {

    /**
     * Return the MetadataID that is being referred to.
     * @return MetadataID
     */
    MetadataID getMetadataID();
    
    /**
     * Set the MetadataID that is being referred to.
     * @param metadataID MetadataID
     */
    void setMetadataID(MetadataID metadataID);    
}
