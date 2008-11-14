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

package com.metamatrix.common.config.api;



/**
 * The ResourceDescriptor represents a resource that is 
 * of a specific ResourceComponentType.  
 * 
 * In the configuration, a ResourceDescriptor will represent
 * a resource pool.
 */

import com.metamatrix.common.config.model.ConfigurationVisitor;


public interface ResourceDescriptor extends ComponentDefn {
    
    /** 
     * The JDBC_RESOURCE_TYPE defines a resource that interacts with a jdbc source
     */  
    public static final String JDBC_RESOURCE_TYPE_NAME = "JDBC Resource Type";    //$NON-NLS-1$

    public static final ComponentTypeID JDBC_RESOURCE_TYPE_ID = new ComponentTypeID(JDBC_RESOURCE_TYPE_NAME);
    
   
   
    /** 
     * The APPSERVER_RESOURCE_TYPE defines a resource that interacts with the app server
     */  
    public static final String APPSERVER_RESOURCE_TYPE_NAME = "AppServerPoolType";    //$NON-NLS-1$

    public static final ComponentTypeID APPSERVER_RESOURCE_TYPE_ID = new ComponentTypeID(APPSERVER_RESOURCE_TYPE_NAME);
   
   void accept(ConfigurationVisitor visitor);

}
