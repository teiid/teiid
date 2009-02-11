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

package com.metamatrix.modeler.core.metadata.runtime;

import java.io.Serializable;

/**
 * RuntimeRecord
 */
public interface MetadataRecord extends Serializable {

    /**
     * Constants for perperties stored on a MetadataRecord 
     * @since 4.3
     */
    public interface MetadataRecordProperties {

        String MODEL_FOR_RECORD  = "modelForRecord";  //$NON-NLS-1$
        String EXTENSIONS_FOR_RECORD  = "extentionsForRecord";  //$NON-NLS-1$
    }

    /**
     * Constants for names of accessor methods that map to fields stored on the MetadataRecords.
     * Note the names do not have "get" on them, this is also the nameInSource
     * of the attributes on SystemPhysicalModel.
     * @since 4.3
     */
    public interface MetadataFieldNames {
        String RECORD_TYPE_FIELD    = "Recordtype"; //$NON-NLS-1$
        String NAME_FIELD           = "Name"; //$NON-NLS-1$
        String FULL_NAME_FIELD      = "FullName"; //$NON-NLS-1$
        String MODEL_NAME_FIELD     = "ModelName"; //$NON-NLS-1$        
        String UUID_FIELD           = "UUID"; //$NON-NLS-1$
        String NAME_IN_SOURCE_FIELD = "NameInSource"; //$NON-NLS-1$
        String PARENT_UUID_FIELD    = "ParentUUID"; //$NON-NLS-1$
    }

    /**
     * Get the UUID of the entity
     * @return the UUID of the entity
     */
    String getUUID();

    /**
     * Get the UUID of the logical parent for the entity.  The
     * logical parent may or may not be the immediate parent
     * for the entity. 
     * @return the UUID of parent for the entity
     */
    String getParentUUID();    

    /**
     * Get entity short name
     * @return short name of the entity
     */
    String getName();

    /**
     * Get entity name in source
     * @return name in source of the entity
     */
    String getNameInSource();

    /**
     * Get path to the resource in the project
     * @return path to the resource in the project
     */
    String getResourcePath();

    /**
     * Get type of the metadata record
     * @return char representing type of the metadata record
     */
    char getRecordType();

    /**
     * Path for the entity. 
     * @return path for the entity
     */
    String getFullName();

    /**
     * The string representation of IPath object for the entity. 
     * @return string form of IPath for the entity
     */
    String getPathString();

    /**
     * IPath object for the entity. 
     * @return IPath for the entity
     */
    String getPath();

    /**
     * Return the name of the model in which this entity exists. 
     * @return name of the containing model
     */
    String getModelName();

    /**
     * Path for the parent of this entity. 
     * @return fullName for the parent of this entity
     * @deprecated the returned fullName may be incorrect in the case of an XML element (see defects #11326 and #11362)
     */
    String getParentFullName();

    /**
     * The string representation of IPath object for the parent entity. 
     * @return string form of IPath for the parent entity
     * @deprecated the returned path may be incorrect in the case of an XML element (see defects #11326 and #11362)
     */
    String getParentPathString();

    /**
     * Return the EObject for the entity this record represents.
     * May be null.
     * @return EObject for the Record.
     */
    Object getEObject();

    /**
     * Get the value of the transient properties that get stored on the
     * records for sake of caching.
     * @param propertyName The name of property (one among those stored on this interface)
     * @return The property value for this property.
     */
    Object getPropertyValue(String propertyName);

    /**
     * Set the value of the transient properties that get stored on the
     * records for sake of caching.
     * @param propertyName The name of property (one among those stored on this interface)
     * @param propertyValue The value of property to be cached.
     */
    void setPropertyValue(String propertyName, Object propertyVame);

}