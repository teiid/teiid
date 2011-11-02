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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extends the logic of Netty's CompactObjectOutputStream to use byte identifiers
 * for some classes and to write/flush objects directly into the output.
 * 
 * We can do this since buffer serialized data is ephemeral and good only for
 * a single process.
 */
public class DataObjectOutputStream extends DataOutputStream implements ObjectOutput {
	
	private static final int MAX_BYTE_IDS = 254;
	static AtomicInteger counter = new AtomicInteger(2);
	static final ConcurrentHashMap<String, Byte> knownClasses = new ConcurrentHashMap<String, Byte>();
	static final ConcurrentHashMap<Byte, String> typeMapping = new ConcurrentHashMap<Byte, String>();
	
    static final int TYPE_FAT_DESCRIPTOR = 0;
    static final int TYPE_THIN_DESCRIPTOR = 1;
    
	ObjectOutputStream oos;
	
	public DataObjectOutputStream(OutputStream out) {
		super(out);
	}

	@Override
	public void writeObject(Object obj) throws IOException {
		if (oos == null) {
			oos = new ObjectOutputStream(this) {
				@Override
				protected void writeStreamHeader() throws IOException {
					writeByte(STREAM_VERSION);
				}
				
				@Override
			    protected void writeClassDescriptor(ObjectStreamClass desc) throws IOException {
			        Class<?> clazz = desc.forClass();
			        if (clazz.isPrimitive() || clazz.isArray()) {
			            write(TYPE_FAT_DESCRIPTOR);
			            super.writeClassDescriptor(desc);
			        } else {
			        	String name = desc.getName();
			        	Byte b = knownClasses.get(name);
			        	if (b == null && counter.get() < MAX_BYTE_IDS) {
		        			synchronized (DataObjectOutputStream.class) {
								b = knownClasses.get(name);
								if (b == null && counter.get() < 254) {
									b = (byte)counter.getAndIncrement();
									knownClasses.put(name, b);
									typeMapping.put(b, name);
								}
			        		}
			        	}
			        	if (b != null) {
			        		write(b);
			        	} else {
				            write(TYPE_THIN_DESCRIPTOR);
				            writeUTF(name);
			        	}
			        }
			    }
			};
		}
		oos.writeObject(obj);
		oos.flush();
	}
	
}
