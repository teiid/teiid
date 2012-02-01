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

package org.teiid.client.xa;

import javax.transaction.xa.XAException;

import org.teiid.core.BundleUtil;
import org.teiid.core.TeiidProcessingException;


/**
 * Exception which occurs if an error occurs within the server that is
 * XA transaction-related.
 */
public class XATransactionException extends TeiidProcessingException {
	private static final long serialVersionUID = 5685144848609237877L;
	private int errorCode = XAException.XAER_RMERR;
    
    public XATransactionException(Throwable e) {
        super(e);
    } 
    
    public XATransactionException(BundleUtil.Event event, int code, Throwable e) {
        super( event, e);
        this.errorCode = code;
    }    
    
    public XATransactionException(BundleUtil.Event event, int code, Throwable e, String msg) {
        super(event, e, msg);
        this.errorCode = code;
    }
    
    public XATransactionException(BundleUtil.Event event, Throwable e) {
        super(event, e);
    }
    
    public XATransactionException(BundleUtil.Event event, int code, String msg) {
        super(event, msg);
        this.errorCode = code;
    } 
    
    public XATransactionException(BundleUtil.Event event, String msg) {
        super(event, msg);
    }    

    public XAException getXAException() {
        Throwable actualException = getCause();
        if (actualException instanceof XAException) {
            return (XAException)actualException;
        }
        return new XAException(errorCode);
    }

} // END CLASS

