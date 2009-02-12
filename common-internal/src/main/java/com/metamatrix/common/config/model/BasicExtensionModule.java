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

package com.metamatrix.common.config.model;

import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.config.api.ResourceDescriptorID;


/** 
 * @since 4.3
 */
public class BasicExtensionModule extends BasicComponentDefn implements ExtensionModule {

    private String type = null;    
    private byte[] contents = null;
    
    public BasicExtensionModule(String name, String type, String description, byte[] contents) {
        super(new ConfigurationID(name), new ExtensionID(name), new ComponentTypeID(name));
        this.type = type;        
        this.contents = contents;
        setDescription(description);    
        
        if (contents == null || type == null) {
            throw new IllegalArgumentException();
        }
    }
    
    public BasicExtensionModule(String name, String description, byte[] contents) {
        super(new ConfigurationID(name), new ExtensionID(name), new ComponentTypeID(name));
        this.type = sniffType(name);        
        this.contents = contents;
        setDescription(description);
        
        if (contents == null ) {
            throw new IllegalArgumentException();
        }        
    }
    

    protected BasicExtensionModule(BasicExtensionModule component) {
        super(component);
    }
        
    /** 
     * @see com.metamatrix.common.config.api.ExtensionModule#getFileContents()
     * @since 4.3
     */
    public byte[] getFileContents() {
        return contents;
    }

    /** 
     * @see com.metamatrix.common.config.api.ExtensionModule#getModuleType()
     * @since 4.3
     */
    public String getModuleType() {
        return type;
    }

    void setModuleType(String type) {
        this.type = type;
    }
    
    void setFileContents(byte[] contents) {
        this.contents= contents;
    }    
 
    /**
     * A temporary resouce id to avoid the . in the name 
     * @since 4.3
     */
    static class ExtensionID extends ResourceDescriptorID{
        public ExtensionID(String name) {
            super(name);
        }
    }
    
    static String sniffType(String name) {
        name = name.toLowerCase();
        
        if (name.endsWith(".jar")) {      //$NON-NLS-1$
            return JAR_FILE_TYPE;
        }
        else if (name.endsWith(".vdb")) { //$NON-NLS-1$
            return JAR_FILE_TYPE;
        }
        else if (name.endsWith(".udf")) { //$NON-NLS-1$
            return FUNCTION_DEFINITION_TYPE;
        }
        return MISC_FILE_TYPE;
    }
}
