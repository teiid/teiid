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

package com.metamatrix.metadata.runtime.api;

import java.util.Collection;
import java.util.Date;

/**
 * <p>The VirtualDatabase identifies a collection of DataSources and Virtual DataSources that a client can connect to and interrogate its metadata.</p> 
 */
public interface VirtualDatabase extends MetadataObject {
/**
 * Return the description.
 * @return String 
 */
    String getDescription();
/**
 * Returns the ModelID's that exist in this virtual database.
 * @return Collection of the ModelID's contained in this VirtualDatabase
 */
    Collection getModelIDs();
/**
 * Returns a short indicating if the status of the VirtualDatabase. There are four
 * status of the VirtualDatabase: "Incomplete", "Inactive",  "Active", and "Deleted".
 * @return boolean true indicates marked for deletion
 */
    short getStatus();
/**
 * Return the global unique identifier for this Virtual Databse. This is the ProjectGUID
 * that identifies the Design Time Configuration that created this VDB.  
 * @return String 
 */
    String getGUID();
/**
 * Return the date the Virtual Database was created.
 * @return Date 
 */
    Date getVersionDate();
/**
 * Return the user name who create this version of the Virtual Database.
 * @return String 
 */
    String getVersionBy();
/**
 * Return the date the Virtual Database was created.
 * @return Date
 */
    Date getCreationDate();
/**
 * Return the user name who create this Virtual Database.
 * @return String
 */
    String getCreatedBy();
/**
 * Return the date the Virtual Database was updated.
 * @return Date
 */
    Date getUpdateDate();
/**
 * Return the user name who update this version of the Virtual Database.
 * @return String
 */
    String getUpdatedBy();
/**
 * Returns the DataTypeID's that exist in this virtual database.
 * @return Collection of the DataTypeID's contained in this VirtualDatabase
 */
    Collection getDataTypeIDs();
/**
 * Update a VDB attribute. Only the attributes defined in <code>VirtualDatabase.ModifiableAttributes</code>
 * can be modefied.
 * @param attribute attribute to be updated.
 * @param value new value.
 */
    void update(String attribute, Object value);

    /**
    * Get this name of the VDB jar file.
    * @return the VDB's name; never null or zero-length
    */
   String getFileName();
   
   /**
    * Returns true if a WSDL is defined for this VDB
    * @return true if a WSDL is defined for this VDB
    */
   boolean hasWSDLDefined();   
   
/**
 * Defines the VDB attributes that can be modified.
 */
    public interface ModifiableAttributes{
        String DESCRIPTION = "description";
    }
}

