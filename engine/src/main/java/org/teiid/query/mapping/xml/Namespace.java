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

package org.teiid.query.mapping.xml;

import java.io.Serializable;


/** 
 * Represents a Single name space in a given XML document 
 */
public class Namespace implements Serializable{
    private String prefix;
    private String uri;
    
    /**
     * This is only in use while building the namespace
     * @param prefix
     */
    public Namespace(String prefix) {
        this.prefix = prefix;
        if (prefix.equalsIgnoreCase(MappingNodeConstants.DEFAULT_NAMESPACE_PREFIX)) {
            this.uri = ""; //$NON-NLS-1$
        }
        else if (prefix.equalsIgnoreCase(MappingNodeConstants.INSTANCES_NAMESPACE_PREFIX)) {
            this.uri = MappingNodeConstants.INSTANCES_NAMESPACE;
        }
    }
    
    public Namespace(String prefix, String uri) {
        this.prefix = prefix;
        if (prefix == null) {
            this.prefix = MappingNodeConstants.DEFAULT_NAMESPACE_PREFIX;
        }
        this.uri = uri;
    }
    
    public String getPrefix() {
        return prefix;
    }
    
    public String getUri() {
        return this.uri;
    }
    
    public void setUri(String uri) {
        this.uri = uri;
    }    
    
    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return prefix + ":" + uri; //$NON-NLS-1$
    }
}
