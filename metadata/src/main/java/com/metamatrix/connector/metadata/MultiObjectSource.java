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

package com.metamatrix.connector.metadata;

import java.util.Collection;
import java.util.Map;

import com.metamatrix.connector.metadata.internal.IObjectSource;

/**
 * Holds two object sources and routes queries to them based on the suffix of the group name.
 */
public class MultiObjectSource implements IObjectSource {
    private IObjectSource primaryObjectSource;
    private IObjectSource secondaryObjectSource;
    private String secondaryGroupNameSuffix;
    
    public MultiObjectSource(IObjectSource primaryObjectSource, String secondaryGroupNameSuffix, IObjectSource objectSource) {
        this.primaryObjectSource = primaryObjectSource;
        this.secondaryGroupNameSuffix = secondaryGroupNameSuffix;
        this.secondaryObjectSource = objectSource;
    }
    
    /* 
     * @see com.metamatrix.connector.metadata.internal.ISimpleObjectSource#getObjects(java.lang.String, java.util.Map)
     */
    public Collection getObjects(String groupName, Map criteria) {
        if (groupName.endsWith(secondaryGroupNameSuffix)) {
            return secondaryObjectSource.getObjects(groupName, criteria);
        }
        return primaryObjectSource.getObjects(groupName, criteria);
    }
}
