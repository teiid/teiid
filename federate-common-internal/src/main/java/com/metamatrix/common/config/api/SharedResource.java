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
 * Date Oct 21, 2002
 *
 * A SharedResource represents a definition of behavior for a
 * non-configuration based object.  This object type is not
 * dependent upon any specific configuration, but when defined,
 * is shared across all configurations.
 */
public interface SharedResource extends ComponentObject {

    
  

	/** 
     * The JDBC_RESOURCE_TYPE resource is the generic resource used for 
     * majority of the internal resources
     */  
    public static final String JDBC_COMPONENT_TYPE_NAME = "JDBC Resource Type";    //$NON-NLS-1$

	public static final ComponentTypeID JDBC_COMPONENT_TYPE_ID = new ComponentTypeID(JDBC_COMPONENT_TYPE_NAME);

	/** The JMS_RESOURCE_TYPE resource is used to define jms resources. */  
    public static final String JMS_COMPONENT_TYPE_NAME = "JMS Resource Type";    //$NON-NLS-1$


	public static final ComponentTypeID JMS_COMPONENT_TYPE_ID = new ComponentTypeID(JMS_COMPONENT_TYPE_NAME);

	/** The MISC_RESOURCE_TYPE resource is used to define resources that need any specific type association. */  
    public static final String MISC_COMPONENT_TYPE_NAME = "Miscellaneous Resource Type";    //$NON-NLS-1$


	public static final ComponentTypeID MISC_COMPONENT_TYPE_ID = new ComponentTypeID(MISC_COMPONENT_TYPE_NAME);

	/** The SEARCHBASE_RESOURCE_TYPE resource is used to define external searchbase resources. */  
    public static final String SEARCHBASE_COMPONENT_TYPE_NAME = "Searchbase Resource Type";    //$NON-NLS-1$


	public static final ComponentTypeID SEARCHBASE_COMPONENT_TYPE_ID = new ComponentTypeID(SEARCHBASE_COMPONENT_TYPE_NAME);
    







}
