package com.metamatrix.common.comm.exception;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.comm.platform.socket.ObjectInputStreamWithClassloader;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ReflectionHelper;

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

public class ExceptionHolder implements Externalizable {
	
	private Throwable exception;
	
	public ExceptionHolder() {
	}
	
	public ExceptionHolder(Throwable exception) {
		this.exception = exception;
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		List<String> classNames = (List<String>)in.readObject();
		String message = (String)in.readObject();
		StackTraceElement[] stackTrace = (StackTraceElement[])in.readObject();
		byte[] serializedException = (byte[])in.readObject();
		ByteArrayInputStream bais = new ByteArrayInputStream(serializedException);
		ObjectInputStream ois = new ObjectInputStreamWithClassloader(bais, Thread.currentThread().getContextClassLoader());
		try {
			this.exception = (Throwable)ois.readObject();
		} catch (ClassNotFoundException e) {
			List<String> args = Arrays.asList(CommPlatformPlugin.Util.getString("ExceptionHolder.converted_exception", message, classNames)); //$NON-NLS-1$
			
			for (String className : classNames) {
				try {
					Throwable result = (Throwable)ReflectionHelper.create(className, args, Thread.currentThread().getContextClassLoader());
					result.initCause(e);
					result.setStackTrace(stackTrace);
					this.exception = result;
					break;
				} catch (MetaMatrixCoreException e1) {
					//
				}
			}
			if (this.exception == null) {
				throw new MetaMatrixRuntimeException(exception, args.get(0));
			}
		}
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		List<String> classNames = new ArrayList<String>();
		Class<?> clazz = exception.getClass();
		while (clazz != null) {
			if (clazz == Throwable.class || clazz == Exception.class) {
				break;
			}
			classNames.add(clazz.getName());
			clazz = clazz.getSuperclass();
		}
		out.writeObject(classNames);
		out.writeObject(exception.getMessage());
		out.writeObject(exception.getStackTrace());
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(this.exception);
        oos.flush();
        oos.close();
		out.writeObject(baos.toByteArray());
	}
	
	public Throwable getException() {
		return exception;
	}
		
}
