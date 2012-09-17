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

package org.teiid.replication.jgroups;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.jgroups.Address;
import org.teiid.core.util.ReflectionHelper;

/**
 * Allows JGroups {@link Address} objects to be serializable
 */
public final class AddressWrapper implements Externalizable {
	
	Address address;
	
	public AddressWrapper() {
		
	}
	
	public AddressWrapper(Address address) {
		this.address = address;
	}
	
	@Override
	public int hashCode() {
		return address.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof AddressWrapper)) {
			return false;
		}
		return address.equals(((AddressWrapper)obj).address);
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		String className = in.readUTF();
		try {
			this.address = (Address) ReflectionHelper.create(className, null, Thread.currentThread().getContextClassLoader());
			this.address.readFrom(in);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(address.getClass().getName());
		try {
			address.writeTo(out);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
}