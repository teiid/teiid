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

package org.teiid.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public final class ObjectInputStreamWithClassloader extends
		ObjectInputStream {
	private final ClassLoader cl;

	public ObjectInputStreamWithClassloader(InputStream in,
			ClassLoader cl) throws IOException {
		super(in);
		this.cl = cl;
	}

	@Override
	protected Class<?> resolveClass(ObjectStreamClass desc)
			throws IOException, ClassNotFoundException {
		//see java bug id 6434149
		try {
			return Class.forName(desc.getName(), false, cl);
		} catch (ClassNotFoundException e) {
			return super.resolveClass(desc);
		}
	}
}