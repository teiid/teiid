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

import java.sql.SQLException;



/**
 * Exception which occurs if an error occurs within the server that is not
 * business-related.  For instance, if a service or bean is not available
 * or communication fails.
 */
public class MetaMatrixCoreException extends Exception {
	
	protected String code;
	
    public MetaMatrixCoreException() {
    }

    public MetaMatrixCoreException(String message) {
        super(message);
    }

    public MetaMatrixCoreException(String errorCode, String message) {
        super(message);
        this.code = errorCode;
    }
    

    public MetaMatrixCoreException(Throwable e) {
        this(e, e.getMessage());        
    }

    public MetaMatrixCoreException(Throwable e, String message) {
        super(message, e);
        setCode(e);
    }
    
    public MetaMatrixCoreException(Throwable e, String errorCode, String message) {
        super(message, e);
        this.code = errorCode;
    }
    
    public String getCode() {
        return this.code;
    }    
    
    private void setCode(Throwable e) {
        if (e instanceof MetaMatrixCoreException) {
            this.code = (((MetaMatrixCoreException) e).getCode());
        } else if (e instanceof MetaMatrixRuntimeException) {
        	this.code = ((MetaMatrixRuntimeException) e).getCode();
        } else if (e instanceof SQLException) {
        	this.code = Integer.toString(((SQLException)e).getErrorCode());
        }
    }
    
	public String getMessage() {
		if (code == null || code.length() == 0) {
			return super.getMessage();
		}
		return "Error Code:"+code+" Message:"+super.getMessage(); //$NON-NLS-1$ //$NON-NLS-2$
	}    
}
