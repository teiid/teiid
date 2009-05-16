package com.metamatrix.api.exception;

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

import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ObjectInputStreamWithClassloader;
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
	private boolean nested = false;
	
	public ExceptionHolder() {
	}
	
	public ExceptionHolder(Throwable exception) {
		this.exception = exception;
	}

	public ExceptionHolder(Throwable exception, boolean nested) {
		this.exception = exception;
		this.nested = nested;
	}
	
	
	//## JDBC4.0-begin ##
	@Override
	//## JDBC4.0-end ##
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		List<String> classNames = (List<String>)in.readObject();;
		String message = (String)in.readObject();
		StackTraceElement[] stackTrace = (StackTraceElement[])in.readObject();
		ExceptionHolder causeHolder = (ExceptionHolder)in.readObject();
		byte[] serializedException = (byte[])in.readObject();
		
		this.exception = readFromByteArray(serializedException);

		if (this.exception == null) {
			Throwable t = buildException(classNames, message, stackTrace);
			if (t == null) {
				if (causeHolder != null) {
					this.exception = causeHolder.exception;
				}
			}
			else {
				if (causeHolder != null) {
					t.initCause(causeHolder.exception);
				}
				this.exception = t;
			}
		}

		if (this.exception == null) {
			this.exception = new MetaMatrixRuntimeException(message);
			this.exception.setStackTrace(stackTrace);
		}
	}
	
	//## JDBC4.0-begin ##
	@Override
	//## JDBC4.0-end ##
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
		
		// specify that this cause is nested exception; not top level
		if (this.exception.getCause() != null && this.exception.getCause() != this.exception) {
			out.writeObject(new ExceptionHolder(this.exception.getCause(), true));
		}
		else {
			out.writeObject(null);
		}
		
		// only for the top level exception write the serialized block for the object
		if (!nested) {
			out.writeObject(writeAsByteArray(this.exception));
		}
		else {
			out.writeObject(null);
		}
	}
	
	public Throwable getException() {
		return exception;
	}
		
	private Throwable buildException(List<String> classNames, String message, StackTraceElement[] stackTrace) {
		List<String> args = Arrays.asList(CorePlugin.Util.getString("ExceptionHolder.converted_exception", message, classNames)); //$NON-NLS-1$
		
		Throwable result = null;
		for (String className : classNames) {
			try {
				result = (Throwable)ReflectionHelper.create(className, args, Thread.currentThread().getContextClassLoader());
				result.setStackTrace(stackTrace);
				break;
			} catch (MetaMatrixCoreException e1) {
				//
			}
		}
		return result;
	}
	
	private byte[] writeAsByteArray(Throwable t) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(t);
        oos.flush();
        oos.close();		
        return baos.toByteArray();
	}
	
	private Throwable readFromByteArray(byte[] contents) throws IOException {
		// only for top level we would have the contents as not null.
		if (contents != null) {
			ByteArrayInputStream bais = new ByteArrayInputStream(contents);
			ObjectInputStream ois = new ObjectInputStreamWithClassloader(bais, Thread.currentThread().getContextClassLoader());
			try {
				return (Throwable)ois.readObject();
			} catch (ClassNotFoundException e) {
				// 
			}
		}
		return null;
	}
	
	public static List<ExceptionHolder> toExceptionHolders(List<? extends Throwable> throwables){
    	List<ExceptionHolder> list = new ArrayList();
    	for (Throwable t: throwables) {
    		list.add(new ExceptionHolder(t));
    	}
    	return list;
	}
	
    public static List<Throwable> toThrowables(List<ExceptionHolder> exceptionHolders) {
    	List<Throwable> list = new ArrayList();
    	for(ExceptionHolder e: exceptionHolders) {
    		list.add(e.getException());
    	}
    	return list;
    }

}
