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

package com.metamatrix.internal.core.xml.xmi;


/** 
 * Class used to store ModelImport information extracted from the ModelAnnotation header
 * @since 4.3
 */
public class ModelImportInfo {

    private String name;
    private String path;
    private String location;
    private String uuid;
    private String modelType;
    private String primaryMetamodelURI;
    
    /** 
     * @return Returns the modelType.
     * @since 4.3
     */
    public String getModelType() {
        return this.modelType;
    }
    
    /** 
     * @param modelType The modelType to set.
     * @since 4.3
     */
    public void setModelType(String modelType) {
        this.modelType = modelType;
    }
    
    /** 
     * @return Returns the name.
     * @since 4.3
     */
    public String getName() {
        return this.name;
    }
    
    /** 
     * @param name The name to set.
     * @since 4.3
     */
    public void setName(String name) {
        this.name = name;
    }
    
    /** 
     * @return Returns the path.
     * @since 4.3
     */
    public String getPath() {
        return this.path;
    }
    
    /** 
     * @param path The path to set.
     * @since 4.3
     */
    public void setPath(String path) {
        this.path = path;
    }
       
    /** 
     * @return Returns the location.
     * @since 4.3
     */
    public String getLocation() {
        return this.location;
    }

    
    /** 
     * @param location The location to set.
     * @since 4.3
     */
    public void setLocation(String location) {
        this.location = location;
    }

    /** 
     * @return Returns the primaryMetamodelURI.
     * @since 4.3
     */
    public String getPrimaryMetamodelURI() {
        return this.primaryMetamodelURI;
    }
    
    /** 
     * @param primaryMetamodelURI The primaryMetamodelURI to set.
     * @since 4.3
     */
    public void setPrimaryMetamodelURI(String primaryMetamodelURI) {
        this.primaryMetamodelURI = primaryMetamodelURI;
    }
    
    /** 
     * @return Returns the uuid.
     * @since 4.3
     */
    public String getUUID() {
        return this.uuid;
    }
    
    /** 
     * @param uuid The uuid to set.
     * @since 4.3
     */
    public void setUUID(String uuid) {
        this.uuid = uuid;
    }
    
    /**
     * Method to print the contents of the ModelImportInfo object.
     * @param stream the stream
     */
    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append("Name: "); //$NON-NLS-1$
        sb.append(this.getName());
        sb.append(", UUID: "); //$NON-NLS-1$
        sb.append(this.getUUID() );
        sb.append(", Path: "); //$NON-NLS-1$
        sb.append(this.getPath() );
        sb.append(", Model location: "); //$NON-NLS-1$
        sb.append(this.getLocation() );
        sb.append(", Model type: "); //$NON-NLS-1$
        sb.append(this.getModelType() );
        sb.append(", Primary Metamodel URI: "); //$NON-NLS-1$
        sb.append(this.getPrimaryMetamodelURI() );
        return sb.toString();
    }

}
