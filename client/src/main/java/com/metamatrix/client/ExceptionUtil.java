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

package com.metamatrix.client;

import java.lang.reflect.Method;

import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.MetaMatrixRuntimeException;

public class ExceptionUtil {
	
    public static <T extends Throwable> T getExceptionOfType(Throwable ex, Class<T> cls) {
        while (ex != null) {
            if (cls.isAssignableFrom(ex.getClass())) {
                return (T)ex;
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
        Class<?>[] exceptionClasses = method.getExceptionTypes();
        for (int i = 0; i < exceptionClasses.length; i++) {
			if (exception.getClass().isAssignableFrom(exceptionClasses[i])) {
				return exception;
			}
			if (MetaMatrixComponentException.class.isAssignableFrom(exceptionClasses[i])) {
				return new MetaMatrixComponentException(exception);	
			}
			if (AdminException.class.isAssignableFrom(exceptionClasses[i])) {
				if (exception instanceof MetaMatrixProcessingException) {
					return new AdminProcessingException(exception);
				}
	        	return new AdminComponentException(exception);
			}
			canThrowXATransactionException |= XATransactionException.class.isAssignableFrom(exceptionClasses[i]);
		}
        if (canThrowXATransactionException) {
        	return new XATransactionException(exception);
        }
        if (RuntimeException.class.isAssignableFrom(exception.getClass())) {
        	return exception;
        }
        return new MetaMatrixRuntimeException(exception);
	}
}
