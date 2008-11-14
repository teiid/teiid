/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.core.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class MixinProxy implements InvocationHandler {
	        
    private Object[] delegates;
    
    public MixinProxy(Object[] delegates) {
        this.delegates = delegates;
    }
    
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
        for (int i = 0; i < delegates.length; i++) {
            Object object = delegates[i];
            Method m = null;
            try {
				m = object.getClass().getDeclaredMethod(method.getName(), method.getParameterTypes());
			} catch (NoSuchMethodException e) {
			}
			if (m != null) {
				try {
					return m.invoke(object, args);
				} catch (InvocationTargetException e) {
					throw e.getTargetException();
				}
			}
        }
        return noSuchMethodFound(proxy, method, args);
    }
    
    protected Object noSuchMethodFound(Object proxy, Method method, Object[] args) throws Throwable {
        throw new RuntimeException("Could not determine target delegate"); //$NON-NLS-1$ 
    }

}
