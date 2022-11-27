/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.core.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidException;


public class ReflectionHelper {

    private Class<?> targetClass;
    private Map<String, LinkedList<Method>> methodMap = null;       // used for the brute-force method finder

    /**
     * Construct a ReflectionHelper instance that cache's some information about
     * the target class.  The target class is the Class object upon which the
     * methods will be found.
     * @param targetClass the target class
     * @throws IllegalArgumentException if the target class is null
     */
    public ReflectionHelper( Class<?> targetClass ) {
        if ( targetClass == null ) {
            throw new IllegalArgumentException(CorePlugin.Util.getString("ReflectionHelper.errorConstructing")); //$NON-NLS-1$
        }
        this.targetClass = targetClass;
    }

    /**
     * Find the best method on the target class that matches the signature specified
     * with the specified name and the list of arguments.  This method first
     * attempts to find the method with the specified arguments; if no such
     * method is found, a NoSuchMethodException is thrown.
     * <P>
     * This method is unable to find methods with signatures that include both
     * primitive arguments <i>and</i> arguments that are instances of <code>Number</code>
     * or its subclasses.
     * @param methodName the name of the method that is to be invoked.
     * @param arguments the array of Object instances that correspond
     * to the arguments passed to the method.
     * @return the Method object that references the method that satisfies
     * the requirements, or null if no satisfactory method could be found.
     * @throws NoSuchMethodException if a matching method is not found.
     * @throws SecurityException if access to the information is denied.
     */
    public Method findBestMethodOnTarget( String methodName, Object[] arguments ) throws NoSuchMethodException, SecurityException {
        createMethodMap();
        List<Method> methods = methodMap.get(methodName);
        if (methods != null && methods.size() == 1) {
            return methods.get(0);
        }

        if (arguments == null) {
            return findBestMethodWithSignature(methodName, Collections.EMPTY_LIST);
        }
        int size = arguments.length;
        List<Class<?>> argumentClasses = new ArrayList<Class<?>>(size);
        for (int i=0; i!=size; ++i) {
            if ( arguments[i] != null ) {
                Class<?> clazz = arguments[i].getClass();
                argumentClasses.add( clazz );
            } else {
                argumentClasses.add(null);
            }
        }
        return findBestMethodWithSignature(methodName,argumentClasses);
    }

    /**
     * Find the best method on the target class that matches the signature specified
     * with the specified name and the list of argument classes.  This method first
     * attempts to find the method with the specified argument classes; if no such
     * method is found, a NoSuchMethodException is thrown.
     * @param methodName the name of the method that is to be invoked.
     * @param argumentsClasses the list of Class instances that correspond
     * to the classes for each argument passed to the method.
     * @return the Method object that references the method that satisfies
     * the requirements, or null if no satisfactory method could be found.
     * @throws NoSuchMethodException if a matching method is not found.
     * @throws SecurityException if access to the information is denied.
     */
    public Method findBestMethodWithSignature( String methodName, Object[] argumentsClasses ) throws NoSuchMethodException, SecurityException {
        List argumentClassesList = Arrays.asList(argumentsClasses);
        return findBestMethodWithSignature(methodName,argumentClassesList);
    }

    /**
     * Find the best method on the target class that matches the signature specified
     * with the specified name and the list of argument classes.  This method first
     * attempts to find the method with the specified argument classes; if no such
     * method is found, a NoSuchMethodException is thrown.
     * @param methodName the name of the method that is to be invoked.
     * @param argumentsClasses the list of Class instances that correspond
     * to the classes for each argument passed to the method.
     * @return the Method object that references the method that satisfies
     * the requirements, or null if no satisfactory method could be found.
     * @throws NoSuchMethodException if a matching method is not found.
     * @throws SecurityException if access to the information is denied.
     */
    public Method findBestMethodWithSignature( String methodName, List<Class<?>> argumentsClasses ) throws NoSuchMethodException, SecurityException {
        // Attempt to find the method
        Method result = null;
        Class[] classArgs = new Class[argumentsClasses.size()];

        // -------------------------------------------------------------------------------
        // First try to find the method with EXACTLY the argument classes as specified ...
        // -------------------------------------------------------------------------------
        try {
            argumentsClasses.toArray(classArgs);
            result = this.targetClass.getMethod(methodName,classArgs);  // this may throw an exception if not found
            return result;
        } catch ( NoSuchMethodException e ) {
            // No method found, so continue ...
        }

        // ---------------------------------------------------------------------------------------------
        // Still haven't found anything.  So far, the "getMethod" logic only finds methods that EXACTLY
        // match the argument classes (i.e., not methods declared with superclasses or interfaces of
        // the arguments).  There is no canned algorithm in Java to do this, so we have to brute-force it.
        // ---------------------------------------------------------------------------------------------
        createMethodMap();

        LinkedList<Method> methodsWithSameName = this.methodMap.get(methodName);
        if ( methodsWithSameName == null ) {
            throw new NoSuchMethodException(methodName);
        }
        for (Method method : methodsWithSameName) {
            Class[] args = method.getParameterTypes();
            boolean allMatch = argsMatch(argumentsClasses, args);
            if ( allMatch ) {
                if (result != null) {
                    throw new NoSuchMethodException(methodName + " Args: " + argumentsClasses + " has multiple possible signatures."); //$NON-NLS-1$ //$NON-NLS-2$
                }
                result = method;
            }
        }

        if (result != null) {
            return result;
        }

        throw new NoSuchMethodException(methodName + " Args: " + argumentsClasses); //$NON-NLS-1$
    }

    private void createMethodMap() {
        if ( this.methodMap == null ) {
            synchronized (this) {
                if (this.methodMap != null) {
                    return;
                }
                HashMap<String, LinkedList<Method>> newMethodMap = new HashMap<String, LinkedList<Method>>();
                Method[] methods = this.targetClass.getMethods();
                for ( int i=0; i!=methods.length; ++i ) {
                    Method method = methods[i];
                    LinkedList<Method> methodsWithSameName = newMethodMap.get(method.getName());
                    if ( methodsWithSameName == null ) {
                        methodsWithSameName = new LinkedList<Method>();
                        newMethodMap.put(method.getName(),methodsWithSameName);
                    }
                    methodsWithSameName.addFirst(method);   // add lower methods first
                }
                this.methodMap = newMethodMap;
            }
        }
    }

    private static boolean argsMatch(List<Class<?>> argumentsClasses, Class[] args) {
        if ( args.length != argumentsClasses.size() ) {
            return false;
        }
        for ( int i=0; i<args.length; ++i ) {
            Class<?> objectClazz = argumentsClasses.get(i);
            if ( objectClazz != null ) {
                Class<?> primitiveClazz = convertArgumentClassesToPrimitive(objectClazz);
                // Check for possible matches with (converted) primitive types
                // as well as the original Object type
                if ( ! args[i].equals(primitiveClazz) && ! args[i].isAssignableFrom(objectClazz) ) {
                    return false;   // found one that doesn't match
                }
            } else {
                // a null is assignable for everything except a primitive
                if ( args[i].isPrimitive() ) {
                    return false;   // found one that doesn't match
                }
            }
        }
        return true;
    }

    /**
     * Convert any argument class to primitive.
     */
    private static Class<?> convertArgumentClassesToPrimitive( Class<?> clazz ) {
        if      ( clazz == Boolean.class   ) { clazz = Boolean.TYPE; }
        else if ( clazz == Character.class ) { clazz = Character.TYPE; }
        else if ( clazz == Byte.class      ) { clazz = Byte.TYPE; }
        else if ( clazz == Short.class     ) { clazz = Short.TYPE; }
        else if ( clazz == Integer.class   ) { clazz = Integer.TYPE; }
        else if ( clazz == Long.class      ) { clazz = Long.TYPE; }
        else if ( clazz == Float.class     ) { clazz = Float.TYPE; }
        else if ( clazz == Double.class    ) { clazz = Double.TYPE; }
        else if ( clazz == Void.class      ) { clazz = Void.TYPE; }
        return clazz;
    }

    /**
     * Helper method to load a class.
     * @param className is the class to instantiate
     * @param classLoader the class loader to use; may be null if the current
     * class loader is to be used
     * @return Class is the instance of the class
     * @throws ClassNotFoundException
     */
    private static final Class<?> loadClass(final String className, final ClassLoader classLoader) throws ClassNotFoundException {
        Class<?> cls = null;
        if ( classLoader == null ) {
            cls = Class.forName(className.trim());
        } else {
            cls = Class.forName(className.trim(),true,classLoader);
        }
        return cls;
    }

    /**
     * Helper method to create an instance of the class using the appropriate
     * constructor based on the ctorObjs passed.
     * @param className is the class to instantiate
     * @param ctorObjs are the objects to pass to the constructor; optional, nullable
     * @param classLoader the class loader to use; may be null if the current
     * class loader is to be used
     * @return Object is the instance of the class
     * @throws TeiidException if an error occurs instantiating the class
     */

    public static final Object create(String className, Collection<?> ctorObjs,
                                      final ClassLoader classLoader) throws TeiidException {
        try {
            int size = (ctorObjs == null ? 0 : ctorObjs.size());
            Class[] names = new Class[size];
            Object[] objArray = new Object[size];
            int i = 0;

            if (size > 0) {
                for (Iterator<?> it=ctorObjs.iterator(); it.hasNext(); ) {
                    Object obj = it.next();
                    if (obj != null) {
                        names[i] = obj.getClass();
                        objArray[i] = obj;
                    }
                    i++;
                }
            }
            return create(className, objArray, names, classLoader);
        } catch (Exception e) {
              throw new TeiidException(CorePlugin.Event.TEIID10033, e);
        }
    }

    public static final Object create(String className, Object[] ctorObjs, Class<?>[] argTypes,
                final ClassLoader classLoader) throws TeiidException {
        Class<?> cls;
        try {
            cls = loadClass(className,classLoader);
        } catch(Exception e) {
              throw new TeiidException(CorePlugin.Event.TEIID10034, e);
        }
        Constructor<?> ctor = null;
        try {
            ctor = cls.getDeclaredConstructor(argTypes);
        } catch (NoSuchMethodException e) {

        }

        if (ctor == null && argTypes != null && argTypes.length > 0) {
            List<Class<?>> argumentsClasses = Arrays.asList(argTypes);
            for (Constructor<?> possible : cls.getDeclaredConstructors()) {
                if (argsMatch(argumentsClasses, possible.getParameterTypes())) {
                    ctor = possible;
                    break;
                }
            }
        }

        if (ctor == null) {
              throw new TeiidException(CorePlugin.Event.TEIID10035, className + CorePlugin.Event.TEIID10035 + Arrays.toString(argTypes));
        }

        try {
            return ctor.newInstance(ctorObjs);
        } catch (InvocationTargetException e) {
            throw new TeiidException(CorePlugin.Event.TEIID10036, e.getTargetException());
        } catch (Exception e) {
            throw new TeiidException(CorePlugin.Event.TEIID10036, e);
        }
    }

}
