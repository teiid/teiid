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

package com.metamatrix.connector.sysadmin;

/**
 * Property names used in the text connector.
 */
public class SysAdminPropertyNames {
    
    public static final String TRIM_STRINGS = "TrimStrings"; //$NON-NLS-1$
    
    /**
     * This is the property name of the ConnectorService property that defines
     * the time zone of the source database.  This property should only be used in 
     * cases where the source database is in a different time zone than the 
     * ConnectorService VM and the database/driver is not already handling 
     * time zones correctly.
     */
    public static final String DATABASE_TIME_ZONE = "DatabaseTimeZone"; //$NON-NLS-1$
       
    
    /**
     * This property is used to specify the implementation of
     * @link com.metamatrix.connector.sysadmin.extension.ISysAdminConnectionFactory
     */
    public static final String SYSADMIN_CONNECTION_FACTORY_CLASS= "SysAdminConnectionFactoryClass"; //$NON-NLS-1$


}
