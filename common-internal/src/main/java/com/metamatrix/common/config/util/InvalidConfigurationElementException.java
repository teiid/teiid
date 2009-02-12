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

package com.metamatrix.common.config.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.metamatrix.api.exception.MetaMatrixException;

public class InvalidConfigurationElementException extends MetaMatrixException{

    private Object invalidElement;
    
    // =========================================================================
    //                       C O N S T R U C T O R S
    // =========================================================================

    /**
     * No-Arg Constructor
     */
    public InvalidConfigurationElementException(  ) {
        super( );
    }    
    /**
     * Construct an instance with the error message specified.
     *
     * @param message The error message
     */
    public InvalidConfigurationElementException( String message, Object invalidElement ) {
        super( message );
        this.invalidElement = invalidElement;
        
    }

    /**
     * Construct an instance with an error code and message specified.
     *
     * @param message The error message
     * @param code    The error code 
     */
    public InvalidConfigurationElementException( String code, String message, Object invalidElement ) {
        super( code, message );
        this.invalidElement = invalidElement;
        
        
    }

    /**
     * Construct an instance with a linked exception specified.
     *
     * @param e An exception to chain to this exception
     */
    public InvalidConfigurationElementException( Throwable e, Object invalidElement  ) {
        super(e);
        this.invalidElement = invalidElement;
        
    }

    /**
     * Construct an instance with a linked exception and error message
     * specified.
     *
     * @param e       An exception to chain to this exception
     * @param message The error message
     */
    public InvalidConfigurationElementException( Throwable e, String message, Object invalidElement  ) {
        super(e, message );
        this.invalidElement = invalidElement;
        
    }

    /**
     * Construct an instance with a linked exception, and an error code and
     * message, specified.
     *
     * @param e       An exception to chain to this exception
     * @param message The error message
     * @param code    The error code 
     */
    public InvalidConfigurationElementException( Throwable e, String code, String message, Object invalidElement  ) {
        super(e, code, message );
        this.invalidElement = invalidElement;
        
    }
    
    public Object getInvalidElement() {
        return invalidElement;
    }
    
    /**
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        invalidElement = in.readObject();
    }

    /**
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        try {
            out.writeObject(invalidElement);
        } catch (Throwable t) {
            out.writeObject(null);
        }
    }
}
