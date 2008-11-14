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

package com.metamatrix.console.ui.views.pools;

import com.metamatrix.common.config.api.ResourceDescriptor;

public class PoolNameAndType {
    private String name;
    private String type;
    private ResourceDescriptor resourceDescriptor;
    
    public PoolNameAndType(String name, String type, ResourceDescriptor rd) {
        super();
        this.name = name;
        this.type = type;
        this.resourceDescriptor = rd;
    }
    
    public String getName() {
        return name;
    }
    
    public String getType() {
        return type;
    }
    
    public ResourceDescriptor getResourceDescriptor() {
        return resourceDescriptor;
    }
    
    public String toString() {
        String str = "PoolNameAndType: name=" + name + ",type=" + type;
        return str;
    }
    
    public boolean equals(Object object) {
        boolean equals;
        if (object == null) {
            equals = false;
        } else if (object == this) {
            equals = true;
        } else if (!(object instanceof PoolNameAndType)) {
            equals = false;
        } else {
            PoolNameAndType pnt = (PoolNameAndType)object;
            equals = (this.name.equals(pnt.getName()) && this.type.equals(
            		pnt.getType()));
        }
        return equals;
    }
}
