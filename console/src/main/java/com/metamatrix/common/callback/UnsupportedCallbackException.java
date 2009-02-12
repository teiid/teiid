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

package com.metamatrix.common.callback;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.metamatrix.api.exception.MetaMatrixException;

/**
 * Exception which occurs if an error occurs within the server that is not
 * business-related.  For instance, if a service or bean is not available
 * or communication fails.
 */
public class UnsupportedCallbackException extends MetaMatrixException {

    private Callback callback;

    /**
     * No-arg costructor required by Externalizable semantics
     */
    public UnsupportedCallbackException() {
        super();
    }
    
    /**
     * Construct an instance with the message specified.
     * @param callback the callback instance that is not supported
     * @param message A message describing the exception
     */
    public UnsupportedCallbackException( Callback callback, String message ) {
        super( message );
        this.callback = callback;
    }

    /**
     * Construct an instance from a message and an exception to chain to this one.
     * @param callback the callback instance that is not supported
     * @param e An exception to nest within this one
     * @param message A message describing the exception
     */
    public UnsupportedCallbackException( Callback callback, Exception e, String message ) {
        super( e, message );
        this.callback = callback;
    }

    public Callback getCallback() {
        return this.callback;
    }

    /**
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        callback = (Callback)in.readObject();
    }

    /**
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        try {
            out.writeObject(callback);
        } catch (Throwable t) {
            out.writeObject(null);
        }
    }

}

