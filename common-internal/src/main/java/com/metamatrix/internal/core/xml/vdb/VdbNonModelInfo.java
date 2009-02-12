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

package com.metamatrix.internal.core.xml.vdb;


/** 
 * Class used to store NonModelReference information extracted from the MetaMatrix-VdbManifestModel.xmi model
 * @since 4.3
 */
public class VdbNonModelInfo {

    private String name;
    private String path;
    private long checkSum;
    
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
     * @return Returns the checkSum.
     * @since 4.3
     */
    public long getCheckSum() {
        return this.checkSum;
    }
 
    /** 
     * @param checkSum The checkSum to set.
     * @since 4.3
     */
    public void setCheckSum(String checkSum) {
        this.checkSum = Long.parseLong(checkSum);
    }
    
    /**
     * Method to print the contents of the VdbModelInfo object.
     * @param stream the stream
     */
    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append("Name: "); //$NON-NLS-1$
        sb.append(this.getName());
        sb.append(", Path: "); //$NON-NLS-1$
        sb.append(this.getPath() );
        sb.append(", checkSum: "); //$NON-NLS-1$
        sb.append(this.getCheckSum() );
        return sb.toString();
    }

}
