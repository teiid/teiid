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

/*
 */
package com.metamatrix.common.vdb.api;

import java.util.Collection;
import java.util.Date;

/**
 * This interface provides the VDB information that can be used to 
 * create VDB.
 */
public interface VDBInfo {

    /**
    * Get this name of this VDB.
    * @return the VDB's name; never null or zero-length
    */
   String getName();

   /**
    * Get this VDB's UUID.
    * @return The UUID as a String.
    */
   String getUUID();

   /**
    * Get this VDB's description.
    * @return The description of this VDB.
    */
   String getDescription();

   /**
    * Get the date this VDB was deployed.
    * @return The VDB's creation date.
    */
   Date getDateCreated();

   /**
    * Get the name of the person who created this VDB.
    * @return The name of the VDB author.
    */
   String getCreatedBy();

   /**
    * Get the Collection of Models this VDB is comprised of.
    * @return This VDB's <code>ModelInfo</code> Collection.
    */
   Collection getModels();
   
   /**
    * Returns the {@link ModelInfo ModelInfo} for the name specified
    * @return ModelInfo
    * 
    */
   ModelInfo getModel(String name);    

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
}
