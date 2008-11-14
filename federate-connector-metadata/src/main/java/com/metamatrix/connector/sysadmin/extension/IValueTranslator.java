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

/*
 */
package com.metamatrix.connector.sysadmin.extension;

import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;

/**
 */
public interface IValueTranslator {
    
    /**
     * Initialize the value translator with the connector's environment, which
     * can be used to retrieve configuration parameters
     * @param env The connector environment
     * @throws ConnectorException If an error occurs during initialization
     */
    void initialize(ConnectorEnvironment env);
    
    /**
     * Returns the <code>Class</code> of the source value. 
     * @return Class of the source value
     * @since 4.3
     */
    Class getSourceType();
    
    /**
     * Returns the <code>Class</code> for which source should be
     * converted to. 
     * @return Class of the target type
     * @since 4.3
     */
    Class getTargetType();
    
    /**
     * Perform the translation from the source type {@see #getSourceType()} to
     * the target type {@see #getTargetType()}. 
     * @param value is the object value to be converted
     * @param command is the executed command
     * @param context is the ExecutionContext
     * @return Object converted to the target type
     * @throws ConnectorException
     * @since 4.3
     */
    Object translate(Object value, IObjectCommand command, ExecutionContext context) throws ConnectorException;
}
