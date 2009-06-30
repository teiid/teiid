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

package org.teiid.adminapi;

import java.util.Date;

import javax.transaction.xa.Xid;

public interface Transaction extends AdminObject {

	/**
	 * Get the session associated with the this transaction.
	 * May be null for an unassociated Global transaction.
	 * @return
	 */
	String getAssociatedSession();
	
	/**
	 * Get the scope for the transaction.  Will be one of GLOBAL, LOCAL, or REQUEST
	 * @return
	 */
	String getScope();
	
	/**
	 * Return the Xid of the Global transaction.  Will be null for non-Global transactions.
	 * @return
	 */
	Xid getXid();
	
	/**
	 * Get the current status.  See {@link javax.transaction.Status} for
	 * possible values.
	 * @return
	 */
	String getStatus();
	
	/**
	 * Get the date the transaction was created.
	 * @return
	 */
	Date getCreated();
	
}
