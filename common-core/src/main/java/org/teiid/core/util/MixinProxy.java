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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class MixinProxy implements InvocationHandler {
	        
	private static class Target {
		Object obj;
		Method m;
	}
	
    private Object[] delegates;
    private Map<Method, Target> methodMap = new HashMap<Method, Target>();
    
    public MixinProxy(Object... delegates) {
        this.delegates = delegates;
    }
    
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
    	Target t = methodMap.get(method);
    	if (t == null) {
            for (int i = 0; i < delegates.length; i++) {
                Object object = delegates[i];
                try {
    				Method m = object.getClass().getMethod(method.getName(), method.getParameterTypes());
    				t = new Target();
    				t.m = m;
    				t.obj = object;
    				methodMap.put(method, t);
    				break;
    			} catch (NoSuchMethodException e) {
    			}
            }
    	}
		if (t != null) {
			try {
				return t.m.invoke(t.obj, args);
			} catch (InvocationTargetException e) {
				throw e.getTargetException();
			}
		}
        return noSuchMethodFound(proxy, method, args);
    }
    
    protected Object noSuchMethodFound(Object proxy, Method method, Object[] args) throws Throwable {
        throw new RuntimeException("Could not determine target delegate for method " + method); //$NON-NLS-1$ 
    }

}
