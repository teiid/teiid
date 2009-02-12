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

package com.metamatrix.console.ui.views.pools;

import com.metamatrix.common.actions.ModificationActionQueue;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;

public class PoolPropertiedObjectAndEditor {
    private String poolName;
    private String poolType;
    private ResourceDescriptor pool;
    private PropertiedObject propertiedObject;
    private PropertiedObjectEditor editor;
    private ModificationActionQueue modificationActionQueue;
    
    public PoolPropertiedObjectAndEditor(String poolName, String poolType,
    		PropertiedObject po, PropertiedObjectEditor poe, 
    		ResourceDescriptor pool, 
    		ModificationActionQueue modificationActionQueue) {
    	super();
    	this.poolName = poolName;
    	this.poolType = poolType;
    	this.propertiedObject = po;
    	this.editor = poe;
    	this.pool = pool;
    	this.modificationActionQueue = modificationActionQueue;
    }
    
    public String getPoolName() {
        return poolName;
    }
    
    public void setPoolName(String name) {
    	poolName = name;
    }
    
    public String getPoolType() {
        return poolType;
    }
    
    public PropertiedObject getPropertiedObject() {
        return propertiedObject;
    }
    
    public PropertiedObjectEditor getEditor() {
        return editor;
    }
    
    public ResourceDescriptor getPool() {
        return pool;
    }

    public ModificationActionQueue getModificationActionQueue() {
        return modificationActionQueue;
    }

    
}
