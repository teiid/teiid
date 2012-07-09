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

/**
 * 
 */
package org.teiid.net.socket;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.teiid.core.util.ArgCheck;
import org.teiid.core.util.ExternalizeUtil;


public final class ServiceInvocationStruct implements Externalizable {
	private static final long serialVersionUID = 1207674062670068350L;
	public Class<?> targetClass;
	public String methodName;
	public Object[] args;
	
	public ServiceInvocationStruct() {
		
	}

	public ServiceInvocationStruct(Object[] args, String methodName,
			Class<?> targetClass) {
		ArgCheck.isNotNull(methodName);
		ArgCheck.isNotNull(targetClass);
		this.args = args;
		this.methodName = methodName;
		this.targetClass = targetClass;
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.targetClass = (Class<?>)in.readObject();
		this.methodName = (String)in.readObject();
		this.args = ExternalizeUtil.readArray(in, Object.class);
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(targetClass);
		out.writeObject(methodName);
		ExternalizeUtil.writeArray(out, args);
	}
	
	@Override
	public String toString() {
		return "Invoke " + targetClass + "." + methodName; //$NON-NLS-1$ //$NON-NLS-2$
	}
}