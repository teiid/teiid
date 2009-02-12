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

package com.metamatrix.api.core.message;

/**
 * Message is intended for situations where there is a need to couple a message's text with a related message type and target
 * object.
 * 
 * @since     3.0
 */
public interface Message {
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Gets the target object to which this message refers.  For example, in the case of an error message, the target would
     * probably refer to the source of the error.
     * @return The message target
     */
    Object getTarget();
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Gets the message text.
     * @return The message text
     */
    String getText();
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Gets the message type.
     * @return The message type
     */
    int getType();
}
