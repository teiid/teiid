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

package com.metamatrix.metadata.runtime.impl;

import java.util.List;

import com.metamatrix.modeler.core.metadata.runtime.VdbRecord;

/**
 * ModelRecordImpl
 */
public class VdbRecordImpl extends AbstractMetadataRecord implements VdbRecord {

    private String version;
    private String identifier;
    private String description;
    private String producerName;
    private String producerVersion;
    private String provider;
    private String timeLastChanged;
    private String timeLastProduced;
    private List modelIDs;

    // ==================================================================================
    //                        C O N S T R U C T O R S
    // ==================================================================================

    public VdbRecordImpl() {
    	this(new MetadataRecordDelegate());
    }
    
    protected VdbRecordImpl(MetadataRecordDelegate delegate) {
    	this.delegate = delegate;
    }

    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.VdbRecord#getDescription()
     */
    public String getDescription() {
        return description;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.VdbRecord#getIdentifier()
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.VdbRecord#getModelIDs()
     */
    public List getModelIDs() {
        return modelIDs;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.VdbRecord#getProducerName()
     */
    public String getProducerName() {
        return producerName;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.VdbRecord#getProducerVersion()
     */
    public String getProducerVersion() {
        return producerVersion;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.VdbRecord#getProvider()
     */
    public String getProvider() {
        return provider;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.VdbRecord#getTimeLastChanged()
     */
    public String getTimeLastChanged() {
        return timeLastChanged;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.VdbRecord#getTimeLastProduced()
     */
    public String getTimeLastProduced() {
        return timeLastProduced;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.VdbRecord#getVersion()
     */
    public String getVersion() {
        return version;
    }

    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    /**
     * @param string
     */
    public void setDescription(String string) {
        description = string;
    }

    /**
     * @param string
     */
    public void setIdentifier(String string) {
        identifier = string;
    }

    /**
     * @param list
     */
    public void setModelIDs(List list) {
        modelIDs = list;
    }

    /**
     * @param string
     */
    public void setProducerName(String string) {
        producerName = string;
    }

    /**
     * @param string
     */
    public void setProducerVersion(String string) {
        producerVersion = string;
    }

    /**
     * @param string
     */
    public void setProvider(String string) {
        provider = string;
    }

    /**
     * @param string
     */
    public void setTimeLastChanged(String string) {
        timeLastChanged = string;
    }

    /**
     * @param string
     */
    public void setTimeLastProduced(String string) {
        timeLastProduced = string;
    }

    /**
     * @param string
     */
    public void setVersion(String string) {
        version = string;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append(getClass().getSimpleName());
        sb.append(" name="); //$NON-NLS-1$
        sb.append(getName());
        sb.append(", version="); //$NON-NLS-1$
        sb.append(getVersion());
        sb.append(", uuid="); //$NON-NLS-1$
        sb.append(getUUID());
        return sb.toString();
    }

}
