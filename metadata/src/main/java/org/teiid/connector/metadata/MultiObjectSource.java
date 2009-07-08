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

package org.teiid.connector.metadata;

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
