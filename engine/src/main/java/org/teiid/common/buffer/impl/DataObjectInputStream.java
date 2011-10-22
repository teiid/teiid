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

package org.teiid.common.buffer.impl;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.io.StreamCorruptedException;

public class DataObjectInputStream extends DataInputStream implements ObjectInput {
	
	ObjectInput ois;
	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	
	public DataObjectInputStream(InputStream in) {
		super(in);
	}
	
	@Override
	public Object readObject() throws ClassNotFoundException, IOException {
		if (ois == null) {
			ois = new ObjectInputStream(this) {
				
				@Override
				protected void readStreamHeader() throws IOException,
						StreamCorruptedException {
					int version = readByte() & 0xFF;
			        if (version != STREAM_VERSION) {
			            throw new StreamCorruptedException("Unsupported version: " + version); //$NON-NLS-1$
			        }
				}
				
			    @Override
			    protected ObjectStreamClass readClassDescriptor()
			            throws IOException, ClassNotFoundException {
			        int type = read();
			        if (type < 0) {
			            throw new EOFException();
			        }
			        switch (type) {
			        case DataObjectOutputStream.TYPE_FAT_DESCRIPTOR:
			            return super.readClassDescriptor();
			        case DataObjectOutputStream.TYPE_THIN_DESCRIPTOR:
			            String className = readUTF();
			            Class<?> clazz = loadClass(className);
			            return ObjectStreamClass.lookup(clazz);
			        default:
			        	className = DataObjectOutputStream.typeMapping.get((byte)type);
			        	if (className == null) {
			        		throw new StreamCorruptedException("Unknown class type " + type); //$NON-NLS-1$
			        	}
			            clazz = loadClass(className);
			            return ObjectStreamClass.lookup(clazz);
			        }
			    }

			    @Override
			    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
			        String className = desc.getName();
			        try {
			            return loadClass(className);
			        } catch (ClassNotFoundException ex) {
			            return super.resolveClass(desc);
			        }
			    }

			    protected Class<?> loadClass(String className) throws ClassNotFoundException {
			        Class<?> clazz;
			        if (classLoader != null) {
			            clazz = classLoader.loadClass(className);
			        } else {
			            clazz = Class.forName(className);
			        }
			        return clazz;
			    }
			};
		}
		return ois.readObject();
	}

}
