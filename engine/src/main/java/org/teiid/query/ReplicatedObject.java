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

package org.teiid.query;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;

/**
 * Optional interface to be implemented by a replicated object to support full and partial state transfer.
 * 
 */
public interface ReplicatedObject {
	
	/**
	 * Allows an application to write a state through a provided OutputStream.
	 *
	 * @param ostream the OutputStream
	 */
	void getState(OutputStream ostream);

	/**
	 * Allows an application to write a partial state through a provided OutputStream.
	 *
	 * @param state_id id of the partial state requested
	 * @param ostream the OutputStream
	 */
	void getState(String state_id, OutputStream ostream);

	/**
	 * Allows an application to read a state through a provided InputStream.
	 * 
	 * @param istream the InputStream
	 */
	void setState(InputStream istream);

	/**
	 * Allows an application to read a partial state through a provided InputStream.
	 *
	 * @param state_id id of the partial state requested
	 * @param istream the InputStream
	 */
	void setState(String state_id, InputStream istream);
	
	/**
	 * Allows the replicator to set the local address from the channel
	 * @param address
	 */
	void setLocalAddress(Serializable address);

	/**
	 * Called when members are dropped
	 * @param addresses
	 */
	void droppedMembers(Collection<Serializable> addresses);
	
}
