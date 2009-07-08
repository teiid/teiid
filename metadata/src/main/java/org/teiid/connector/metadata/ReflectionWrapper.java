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

package org.teiid.connector.metadata;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 * Wraps a target object to allow methods on the target object to be accessed via
 * a generic "get" and "set" methods.
 */
public class ReflectionWrapper {
    
    private final Object target;
    private final Class targetClass;

    public ReflectionWrapper(final Object target) {
        this.target = target;
        this.targetClass = target.getClass();
    }
    
    /**
     * The get method takes a property name and attempts to find a matching method on the target class.
     * If a matching method is found then the method is invoked and the results are returned.
     */
    public Object get(final String propertyName) {
        Method method = null;
        try {
            method = targetClass.getMethod(propertyName, new Class[0]);
        } catch (NoSuchMethodException e) {
            try {
				method = targetClass.getMethod(MetadataConnectorConstants.GET_METHOD_PREFIX + propertyName, new Class[0]);
			} catch (NoSuchMethodException e1) {
				throw new MetaMatrixRuntimeException(e1);
			} 
        }
        try {
            return method.invoke(target, new Object[0]);
        } catch (IllegalAccessException e) {
            throw new MetaMatrixRuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }

    /**
     * The set method takes a property name and value and attempts to find a matching method on the target class.
     * If a matching method is found then the method is invoked and the results are returned.
     */
    public Object set(final String propertyName, final Object[] propertyValues) {
        Method method = null;
        // get parameter types
        Class[] types = null;
        if(propertyValues != null) {
	        types = new Class[propertyValues.length];
	        for(int i=0; i<propertyValues.length; i++) {
	            types[i] = propertyValues[i].getClass();
	        }
        }            
        
        try {
            method = targetClass.getMethod(propertyName, types);
        } catch (NoSuchMethodException e) {
        	try {
				method = targetClass.getMethod(MetadataConnectorConstants.SET_METHOD_PREFIX + propertyName, types);
			} catch (NoSuchMethodException e1) {
				throw new MetaMatrixRuntimeException(e1);
			}
        }
        try {
            // execute the method with arguments
            return method.invoke(target, propertyValues);
        } catch (IllegalAccessException e) {
            throw new MetaMatrixRuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new MetaMatrixRuntimeException(e);
        }
    }    
}
