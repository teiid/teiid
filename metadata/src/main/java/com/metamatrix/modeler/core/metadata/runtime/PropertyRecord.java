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

/**
 * PropertyRecord
 */
public interface PropertyRecord extends MetadataRecord {

    /**
     * Constants for names of accessor methods that map to fields stored  on the PropertyRecords.
     * Note the names do not have "get" on them, this is also the nameInsource
     * of the attributes on SystemPhysicalModel.
     * @since 4.3
     */
    public interface MetadataFieldNames {
        String PROPERTY_NAME_FIELD    = "PropertyName"; //$NON-NLS-1$
        String PROPERTY_VALUE_FIELD    = "PropertyValue"; //$NON-NLS-1$
    }

    /**
     * Return the property name for this record
     * @return property name
     */
    String getPropertyName();

    /**
     * Return the property value for this record
     * @return property value
     */
    String getPropertyValue();

    /**
     * Bollean indiacting if this is an extention property 
     * @return true if it is an extention property
     * @since 4.2
     */
    boolean isExtension();
}
