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

package com.metamatrix.admin.objects;

import org.teiid.adminapi.ExtensionModule;

import com.metamatrix.admin.AdminPlugin;

/**
 * A simple Extension Modules for the Admin API
 */
public final class MMExtensionModule extends MMAdminObject implements ExtensionModule {

    private String description;
    private byte[] fileContents;
    private String moduleType;

    
   
    /**
     * Create a new MMExtensionModule 
     * @param identifierParts
     * @since 4.3
     */
    public MMExtensionModule(String[] identifierParts) {
        super(identifierParts);   
    }
    
    
    
    /**
     * @return description
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * @param description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * @return byte array of file contents
     */
    public byte[] getFileContents() {
        return fileContents;
    }
    
    /**
     * @param contents
     */
    public void setFileContents(byte[] contents) {
        this.fileContents = contents;
    }
    
    /**
     * @return String of the Module Type for this Extension Module
     */
    public String getModuleType() {
        return moduleType;
    }
    
    /**
     * @param type
     */
    public void setModuleType(String type) {
        this.moduleType = type;
    }

    

    /**
     * @return a String of this object for display.
     */
    public String toString() {
        StringBuffer result = new StringBuffer();
        result.append(AdminPlugin.Util.getString("MMExtensionModule.MMExtensionModule")).append(getIdentifier()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMExtensionModule.moduleType")).append(getModuleType()); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("MMExtensionModule.description")).append(getDescription()); //$NON-NLS-1$
        return result.toString();
    }
    
    
    

}