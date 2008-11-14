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

package com.metamatrix.common.config.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.metamatrix.api.exception.MetaMatrixException;

public class ConfigObjectsNotResolvableException extends MetaMatrixException{

    private Object notResolvableObject;
    
    // =========================================================================
    //                       C O N S T R U C T O R S
    // =========================================================================

    /**
     * No-arg costructor required by Externalizable semantics
     */
    public ConfigObjectsNotResolvableException() {
        super();
    }
    
    /**
     * Construct an instance with the error message specified.
     *
     * @param message The error message
     */
    public ConfigObjectsNotResolvableException( String message, Object notResolvableObject ) {
        super( message );
        this.notResolvableObject = notResolvableObject;
        
    }

    /**
     * Construct an instance with an error code and message specified.
     *
     * @param message The error message
     * @param code    The error code 
     */
    public ConfigObjectsNotResolvableException( String code, String message, Object notResolvableObject ) {
        super( code, message );
        this.notResolvableObject = notResolvableObject;
        
        
    }

    /**
     * Construct an instance with a linked exception specified.
     *
     * @param e An exception to chain to this exception
     */
    public ConfigObjectsNotResolvableException( Throwable e, Object notResolvableObject ) {
        super(e);
        this.notResolvableObject = notResolvableObject;
        
    }

    /**
     * Construct an instance with a linked exception and error message
     * specified.
     *
     * @param e       An exception to chain to this exception
     * @param message The error message
     */
    public ConfigObjectsNotResolvableException( Throwable e, String message, Object notResolvableObject ) {
        super(e, message );
        this.notResolvableObject = notResolvableObject;
        
    }

    /**
     * Construct an instance with a linked exception, and an error code and
     * message, specified.
     *
     * @param e       An exception to chain to this exception
     * @param message The error message
     * @param code    The error code 
     */
    public ConfigObjectsNotResolvableException( Throwable e, String code, String message, Object notResolvableObject ) {
        super(e, code, message );
        this.notResolvableObject = notResolvableObject;
        
    }
    
    public Object getNotResolvableObject() {
        return notResolvableObject;
    }
    /**
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        notResolvableObject = in.readObject();
    }

    /**
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        try {
            out.writeObject(notResolvableObject);
        } catch (Throwable t) {
            out.writeObject(null);
        }
    }

}
