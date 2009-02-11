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

import com.metamatrix.modeler.core.metadata.runtime.PropertyRecord;

/**
 * PropertyRecordImpl
 */
public class PropertyRecordImpl extends AbstractMetadataRecord implements PropertyRecord {

    private String name;
    private String value;
    // its true for old vdbs(version < RuntimeAdapter.ANNOTATION_TAGS_INDEX_VERSION), since they represent only extentions
    private boolean isExtention = true;

    public PropertyRecordImpl() {
    	this(new MetadataRecordDelegate());
    }

    protected PropertyRecordImpl(MetadataRecordDelegate delegate) {
    	this.delegate = delegate;
    }
    
    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.PropertyRecord#getPropertyName()
     */
    public String getPropertyName() {
        return this.name;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.PropertyRecord#getPropertyValue()
     */
    public String getPropertyValue() {
        return this.value;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.PropertyRecord#isExtention()
     * @since 4.2
     */
    public boolean isExtension() {
        return this.isExtention;
    }

    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append(getClass().getSimpleName());
        sb.append(", uuid="); //$NON-NLS-1$
        sb.append(getUUID());
        sb.append(" propName="); //$NON-NLS-1$
        sb.append(getPropertyName());
        sb.append(" propValue="); //$NON-NLS-1$
        sb.append(getPropertyValue());
        sb.append(", pathInModel="); //$NON-NLS-1$
        sb.append(getPath());
        return sb.toString();
    }

    /**
     * @param list
     */
    public void setPropertyName(final String name) {
        this.name = name;
    }

    /**
     * @param list
     */
    public void setPropertyValue(final String value) {
        this.value = value;
    }

    /**
     * @param isExtention The isExtention to set.
     * @since 4.2
     */
    public void setExtension(boolean isExtention) {
        this.isExtention = isExtention;
    }
}