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

package com.metamatrix.modeler.core.metadata.runtime;

import java.util.List;

/**
 * ModelRecord
 */
public interface VdbRecord extends MetadataRecord {
    
    /**
     * Return the version of the VDB archive
     * @return
     */
    String getVersion();
    
    /**
     * Return the identifier for the VDB archive
     * @return
     */
    String getIdentifier();
    
    /**
     * Return the description for the VDB archive
     * @return
     */
    String getDescription();
    
    /**
     * Return the name of the VDB archive producer
     * @return
     */
    String getProducerName();
    
    /**
     * Return the version of the VDB archive producer
     * @return
     */
    String getProducerVersion();
    
    /**
     * Return the name of the provider
     * @return
     */
    String getProvider();
    
    /**
     * Return the time the VDB archive was last changed
     * @return
     */
    String getTimeLastChanged();
    
    /**
     * Return the time the VDB archive was last re-indexed
     * @return
     */
    String getTimeLastProduced();
    
    /**
     * Return the list of model identifiers for the VDB archive
     * @return
     */
    List getModelIDs();

}
