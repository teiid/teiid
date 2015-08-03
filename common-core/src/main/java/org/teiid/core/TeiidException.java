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

package org.teiid.core;

import java.sql.SQLException;



/**
 * Exception which occurs if an error occurs within the server that is not
 * business-related.  For instance, if a service or bean is not available
 * or communication fails.
 */
public class TeiidException extends Exception {
	
	private static final long serialVersionUID = -3033427629587497938L;
	protected String code;
	private transient String originalType;
	
    public TeiidException() {
    }

    public TeiidException(String message) {
        super(message);
    }
    
    public TeiidException(BundleUtil.Event code, final String message) {
        super(message);
        setCode(code.toString());
    }  
    
    public TeiidException(BundleUtil.Event code, Throwable t, final String message) {
        super(message, t);
        if (message != null && t != null && message.equals(t.getMessage())) {
        	setCode(code, t);
        } else {
        	setCode(code.toString());
        }
    }  
    
    public TeiidException(BundleUtil.Event code, Throwable t) {
        super(t);
        setCode(code, t);
    }

	private void setCode(BundleUtil.Event code, Throwable t) {
		String codeStr = code.toString();
        if (t instanceof TeiidException) {
        	TeiidException te = (TeiidException)t;
        	if (te.getCode() != null) {
        		codeStr = te.getCode();
        	}
        }
        setCode(codeStr);
	}    

    public TeiidException(Throwable e) {
        this(e, e != null? e.getMessage() : null);        
    }

    public TeiidException(Throwable e, String message) {
        super(message, e);
        setCode(getCode(e));
    }
    
    public String getCode() {
        return this.code;
    }    
    
    public void setCode(String code) {
    	this.code = code;
    }
    
    public String getOriginalType() {
		return originalType;
	}
    
    public void setOriginalType(String originalType) {
		this.originalType = originalType;
	}
    
    static String getCode(Throwable e) {
        if (e instanceof TeiidException) {
            return (((TeiidException) e).getCode());
        } else if (e instanceof TeiidRuntimeException) {
        	return ((TeiidRuntimeException) e).getCode();
        } else if (e instanceof SQLException) {
        	return ((SQLException)e).getSQLState();
        }
        return null;
    }
    
	public String getMessage() {
		String message = super.getMessage();
		if (message == null) {
			return code;
		}
		if (code == null || code.length() == 0 || message.startsWith(code)) {
			return message;
		}
		return code+" "+message; //$NON-NLS-1$
	} 
	
}
