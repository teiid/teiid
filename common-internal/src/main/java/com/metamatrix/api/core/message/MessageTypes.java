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

//################################################################################################################################
package com.metamatrix.api.core.message;

/**
 * Contains the pre-defined message types for the {@link DefaultMessage} create methods.
 * @since 3.0
 */
public interface MessageTypes {
	//############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    /**
     * Type for "null" messages
     * @since 3.0
     */
    int NULL_MESSAGE			= 0;
    
    /**
     * Type for notification messages
     * @since 3.0
     */
    int NOTIFICATION_MESSAGE	= 1;
    
    /**
     * Type for warning messages
     * @since 3.0
     */
    int WARNING_MESSAGE			= 2;
    
    /**
     * Type for error messages
     * @since 3.0
     */
    int ERROR_MESSAGE			= 3;
}
