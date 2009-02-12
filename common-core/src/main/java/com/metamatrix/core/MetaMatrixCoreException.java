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

package com.metamatrix.core;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;

import com.metamatrix.common.util.exception.SQLExceptionUnroller;


/**
 * Exception which occurs if an error occurs within the server that is not
 * business-related.  For instance, if a service or bean is not available
 * or communication fails.
 */
public class MetaMatrixCoreException extends Exception {
	
	private transient Throwable realCause;

    public MetaMatrixCoreException() {
    }

    public MetaMatrixCoreException(String message) {
        super(message);
    }

    public MetaMatrixCoreException(Throwable e) {
        this(e, e.getMessage());
    }

    public MetaMatrixCoreException(Throwable e, String message) {
        super(message);
        this.realCause = e;
    }
    
    @Override
    public Throwable getCause() {
    	return this.realCause;
    }
    
    @Override
    public synchronized Throwable initCause(Throwable cause) {
    	if (this.realCause != null)
            throw new IllegalStateException("Can't overwrite cause"); //$NON-NSL-1$
        if (cause == this)
            throw new IllegalArgumentException("Self-causation not permitted"); //$NON-NSL-1$
        this.realCause = cause;
        return this;
    }
    
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    	in.defaultReadObject();
    	try {
    		realCause = (Throwable)in.readObject();
    	} catch (ClassNotFoundException cnfe) {
    		realCause = new MetaMatrixCoreException(cnfe, CorePlugin.Util.getString("MetaMatrixException.deserialization_exception")); //$NON-NSL-1$
    	}
    }
    
    private void writeObject(ObjectOutputStream out) throws IOException {
    	getStackTrace();
    	out.defaultWriteObject();
    	if (realCause != this && realCause instanceof SQLException) {
    		out.writeObject(SQLExceptionUnroller.unRollException((SQLException)realCause));
    	} else {
    		out.writeObject(realCause);
    	}
    }
}
