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

package org.teiid.client.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.teiid.client.xa.XATransactionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;


public class ExceptionUtil {
	
	public static <T extends Throwable> T getExceptionOfType(Throwable ex, Class<T> cls) {
        while (ex != null) {
            if (cls.isAssignableFrom(ex.getClass())) {
                return cls.cast(ex);
            } 
            if (ex.getCause() == ex) {
            	break;
            }
            ex = ex.getCause();
        }
        return null;
    }
	
	public static Throwable convertException(Method method, Throwable exception) {
		boolean canThrowXATransactionException = false;
		boolean canThrowComponentException = false;
        Class<?>[] exceptionClasses = method.getExceptionTypes();
        for (int i = 0; i < exceptionClasses.length; i++) {
			if (exceptionClasses[i].isAssignableFrom(exception.getClass())) {
				return exception;
			}
			if (!canThrowComponentException) {
				canThrowComponentException = TeiidComponentException.class.isAssignableFrom(exceptionClasses[i]);
			}
			if (!canThrowXATransactionException) {
				canThrowXATransactionException = XATransactionException.class.isAssignableFrom(exceptionClasses[i]);
			}
        }
        if (canThrowComponentException) {
        	return new TeiidComponentException(exception);
        }
        if (canThrowXATransactionException) {
        	return new XATransactionException(exception);
        }
        if (RuntimeException.class.isAssignableFrom(exception.getClass())) {
        	return exception;
        }
        return new TeiidRuntimeException(exception);
	}
	
	/**
	 * Strip out the message and optionally the stacktrace 
	 * @param t
	 * @return
	 */
	public static Throwable sanitize(Throwable t, boolean perserveStack) {
		String code = null;
    	if (t instanceof TeiidException) {
    		code = ((TeiidException)t).getCode();
    	} else if (t instanceof TeiidRuntimeException) {
    		code = ((TeiidRuntimeException)t).getCode();
    	} else {
    		code = t.getClass().getName();
    	}
    	Throwable child = null;
		if (t.getCause() != null && t.getCause() != t) {
			child = sanitize(t.getCause(), perserveStack);
		}
		Class<?> clazz = t.getClass();
		Throwable result = null;
		while (clazz != null) {
			if (clazz == Throwable.class || clazz == Exception.class) {
				break;
			}
	        Constructor<?> ctor = null;
	        try {
	        	ctor = clazz.getDeclaredConstructor(new Class<?>[] {String.class});
				result = (Throwable) ctor.newInstance(code);
				break;
	        } catch (Exception e) {
	        	
	        }
			clazz = clazz.getSuperclass();
		}
		if (result == null) {
			result = new TeiidException(code);
		}
		if (result instanceof TeiidException) {
			((TeiidException)result).setCode(code);
		} else if (result instanceof TeiidRuntimeException) {
			((TeiidException)result).setCode(code);
		}
		if (child != null) {
			result.initCause(child);
		}
		if (perserveStack) {
			result.setStackTrace(t.getStackTrace());
		} else {
			result.setStackTrace(new StackTraceElement[0]);
		}
		return result;
	}
}
