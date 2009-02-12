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

package com.metamatrix.console.ui.views.vdb;

/** 
 * Data class to hold the connector binding(s) for a model.
 * 
 * @since 4.2
 */
public class VDBConnectorBindingNames {
    private String modelName;
    private ConnectorBindingNameAndUUID[] bindings;
    private boolean multiSource;
    
    public VDBConnectorBindingNames(String mdlName, ConnectorBindingNameAndUUID[] bndgs, 
            boolean multiSrc) {
        super();
        this.modelName = mdlName;
        this.bindings = bndgs;
        this.multiSource = multiSrc;
    }
    
    public String getModelName() {
        return modelName;
    }
    
    public ConnectorBindingNameAndUUID[] getBindings() {
        return bindings;
    }
    
    public void setBindings(ConnectorBindingNameAndUUID[] newBindings) {
        this.bindings = newBindings;
    }
    
    public boolean isMultiSource() {
        return multiSource;
    }
    
    public void setMultiSource(boolean flag) {
        multiSource = flag;
    }
    
    public VDBConnectorBindingNames copy() {
        return new VDBConnectorBindingNames(getModelName(), getBindings(), isMultiSource());
    }
}
