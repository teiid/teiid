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
package org.teiid.rhq.comm;

import java.util.Map;


/** 
 * @since 4.3
 */
public interface Component extends Comparable<Object> {
    
    public static final String SYSTEM_KEY = "teiid.system.key"; //$NON-NLS-1$
    public static final String NAME = "teiid.name"; //$NON-NLS-1$
    public static final String IDENTIFIER = "teiid.identifier"; //$NON-NLS-1$
    public static final String DESCRIPTION = "teiid.description"; //$NON-NLS-1$
    public static final String VERSION = "teiid.version"; //$NON-NLS-1$
    
    /**
     * Return the system key that this component identifies with.
     * @return String system key
     */
    String getSystemKey();
    
    /**
     * Return the name for this component
     * @return String name
     */
    String getName();
    
    /**
     * return the unique identifier for this component
     * @return String unique identifier
     */
    String getIdentifier();
    
    /**
     * Return the description
     * @return String description
     */
    String getDescription();
    
    /**
     * Return the version
     * @return String version
     */
    String getVersion();
    
    /**
     * Return a value for the request property key
     * @param key is the identifier to look for
     * @return String value
     */
    String getProperty(String key);
    
    
    /**
     * Return the map of properties.
     * @return Map of properties
     */
    Map getProperties();

    
    

}
