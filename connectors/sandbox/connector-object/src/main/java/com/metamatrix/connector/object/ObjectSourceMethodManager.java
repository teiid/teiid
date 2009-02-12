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

package com.metamatrix.connector.object;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.metamatrix.connector.object.extension.IObjectCommand;
import com.metamatrix.core.util.ReflectionHelper;


/** 
 * @since 4.3
 */
public class ObjectSourceMethodManager {
    
    private Class api = null;
    private Method[] apiMethods = null;
                  
    
    public ObjectSourceMethodManager(Class api) throws SecurityException{
        this.api = api;
        apiMethods = getMethodsFromAPI(this.api);
    }

    public Method getMethodFromAPI(IObjectCommand command) throws SecurityException, NoSuchMethodException {
        return getMethod(this.api, command.getGroupName(), command.getCriteriaTypes(), apiMethods);
    }
    
//    public Method getMethodFromAPI(String methodName, List parms) throws SecurityException, NoSuchMethodException {
//        return getMethod(this.api, methodName, parms, apiMethods);
//    }
    

//    public static Method getMethodFromAPI(Class api, IObjectCommand command) throws SecurityException, NoSuchMethodException {
//      Method[] methods = getMethodsFromAPI(api);
//      return getMethod(api, command.getGroupName(), command.getCriteriaTypes(), methods);
//    }
    
    /**
     * Called to obtain a method from object class, in this case the object is a result object and trying to find
     * the "getter" method to obtain a value
     * @param api
     * @param methodName
     * @param parms
     * @return
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @since 4.3
     */
    public static Method getMethodFromObject(Class object, String methodName, List argumentsClasses) throws SecurityException, NoSuchMethodException {
        Method[] methods = getMethodsFromAPI(object);
        return getMethod(object, methodName, argumentsClasses, methods);
    }
    
    /**
     * Call to execute the method 
     * @param m is the method to execute
     * @param api is the object to execute the method on 
     * @param parms are the parameters to pass when the method is executed
     * @return Object return value
     * @throws Exception
     * @since 4.3
     */
    public static Object executeMethod(Method m, Object api, List parms) throws Exception {
        
        return m.invoke(api, parms.toArray());
    }    
        
        
    private static Method getMethod(Class api, String methodName, List argumentsClasses, Method[] methods) throws SecurityException, NoSuchMethodException {
        Method[] namedMethods = findMethods(methodName, methods);
         if (namedMethods != null && namedMethods.length ==1) {
                return namedMethods[0];
        }
         
        Method m = findBestMethod(api, methodName, argumentsClasses); 
                
        if (m==null) {
            throw new NoSuchMethodException(ObjectPlugin.Util.getString("ObjectSourceMethodManager.No_method_implemented_for", methodName)); //$NON-NLS-1$           
        }
        return m;
    }    
    
    
    
    private static Method[] findMethods( String methodName, Method[] methods ) {
        if (methods == null || methods.length == 0) {
            return null;
        }
         final ArrayList result = new ArrayList();
        for (int i = 0; i < methods.length; i++) {
            final Method m = methods[i];
            if ( m.getName().equals(methodName) ) {
                result.add(m);
            }
        }
        return (Method[]) result.toArray(new Method[result.size()]);
    }
    
    private static Method findBestMethod(Class objectClass, String methodName, List argumentsClasses) throws SecurityException, NoSuchMethodException {
        ReflectionHelper rh = new ReflectionHelper(objectClass);
        
        if (argumentsClasses == null) {
            argumentsClasses = Collections.EMPTY_LIST;
        }
        Method m = rh.findBestMethodWithSignature(methodName, argumentsClasses);
        return m;
       
    }
    
    public static Method[] getMethodsFromAPI(Class api) throws SecurityException {
        final Method[] methods = api.getMethods();
        return methods;
        
    }

}
