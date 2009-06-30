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

package com.metamatrix.admin.objects;

import javax.transaction.xa.Xid;

import org.teiid.adminapi.Transaction;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.common.xa.MMXid;

public class TransactionImpl extends MMAdminObject implements Transaction {
	
	private String associatedSession;
	private String scope;
	private MMXid xid;
	private String status;
	
	public TransactionImpl(String ... id) {
		super(id);
	}

	public String getAssociatedSession() {
		return associatedSession;
	}

	public void setAssociatedSession(String associatedSession) {
		this.associatedSession = associatedSession;
	}

	public String getScope() {
		return scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}

	public Xid getXid() {
		return xid;
	}

	public void setXid(MMXid xid) {
		this.xid = xid;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Override
	public String toString() {
        StringBuffer result = new StringBuffer();
        result.append(AdminPlugin.Util.getString("TransactionImpl.identifier")).append(getIdentifier());  //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("TransactionImpl.associatedSession")).append(associatedSession); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("TransactionImpl.scope")).append(scope); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("TransactionImpl.status")).append(status); //$NON-NLS-1$
        result.append(AdminPlugin.Util.getString("TransactionImpl.xid")).append(xid); //$NON-NLS-1$
        return result.toString();
	}

}
