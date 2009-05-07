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

package com.metamatrix.common.classloader;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * Utility methods for dealing with class loaders.
 * For now, this code is located here so that the class loading driver code will not reference anything in other packages.
 */
public class ClassLoaderUtil {
    public static boolean debug = false;
    
    /**
     * Walk through the chain of class loaders, printing information about where each will load classes from
     * (if they are subclasses of URLClassLoader).
     */
    public static String getClassLoaderInformation(ClassLoader classLoader, String label) {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(byteStream);
        stream.println( "START CLASS LOADERS - " + label); //$NON-NLS-1$
        getClassPathRecursively( classLoader, stream );
        stream.println( "END CLASS LOADERS - " + label); //$NON-NLS-1$
        stream.flush();
        return byteStream.toString();
    }
    
    private static void getClassPathRecursively(ClassLoader classLoader, PrintStream stream) {
        getClassPath(classLoader, stream);
        if (classLoader.getParent() != null) {
            getClassPathRecursively(classLoader.getParent(), stream);
        }
    }
    
    private static void getClassPath(ClassLoader classLoader, PrintStream stream) {
        stream.println( "ClassLoader: " + classLoader); //$NON-NLS-1$
        if (classLoader instanceof URLClassLoader) {
            URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
            URL[] urls = urlClassLoader.getURLs();
            for (int i=0; i<urls.length; i++) {
                stream.println( urls[i].toString() );
            }
        }
    }
    
    private static void log(String label, String value) {
        if (debug) {
            System.out.println("ClassLoaderUtil " + label + " = " + value); //$NON-NLS-1$  //$NON-NLS-2$
        }
    }
    
    /**
     * Create an instance of the specified class (using the default constructor) in a new class loader defined by the provided class path.
     * The new class loader's parent will be the extension class loader.
     */
    public static Object getInstanceInNewClassLoader(String className, String classPath) {
        log("className", className); //$NON-NLS-1$
        log("classPath", classPath); //$NON-NLS-1$

        ClassLoader classLoader = new URLClassLoader(toUrls(classPath), getParentClassLoader());

        try {
            Class clazz = classLoader.loadClass(className);
            Object result = clazz.newInstance();
            if (debug) {
                log("ClassLoaderUtil.getInstanceInNewClassLoader classLoader information", //$NON-NLS-1$
                    getClassLoaderInformation(result.getClass().getClassLoader(),
                    "object loaded by ClassLoaderUtil.getInstanceInNewClassLoader")); //$NON-NLS-1$
            }
            return result;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static ClassLoader getParentClassLoader() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        ClassLoader parent = loader.getParent();
        while (parent != null) {
            loader = parent;
            parent = loader.getParent();
        }
        return loader;
    }

    private static URL[] toUrls(String classPath) {
        List urls = new ArrayList();
        StringTokenizer tokenizer = new StringTokenizer(classPath, System.getProperty("path.separator")); //$NON-NLS-1$
        while (tokenizer.hasMoreElements()) {
            String path = tokenizer.nextToken();
            try {
                urls.add( new File(path).toURL() );
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }
        return (URL[]) urls.toArray( new URL[] {} );
    }

    public static String readResource(String resourceName) {
        StringWriter result = new StringWriter();
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
        if (inputStream == null) {
            throw new RuntimeException("Resource not found: " + resourceName); //$NON-NLS-1$
        }
        BufferedReader reader = null;
        try {
            try {
                reader = new BufferedReader(new InputStreamReader(inputStream));
                char[] buffer = new char[1024];
                while (reader.ready()) {
                    int bytesRead = reader.read(buffer);
                    result.write(buffer, 0, bytesRead);
                }
                return result.toString();
            } finally {
                if(reader != null) {
                    reader.close();                    
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * This method assumes implementation knowledge of the class loaders 
     * (it accesses a private field in java.lang.ClassLoader).
     */
    public static void printLoadedClasses(ClassLoader classLoader, PrintStream stream) {
        Class classLoaderClass = classLoader.getClass();
        while (classLoaderClass != java.lang.ClassLoader.class) {
            classLoaderClass = classLoaderClass.getSuperclass();
        }
        try {
            Field classesField = classLoaderClass.getDeclaredField("classes"); //$NON-NLS-1$
            classesField.setAccessible(true);
            Vector classes = (Vector) classesField.get(classLoader);
            for (Iterator i = classes.iterator(); i.hasNext(); ) {
                Class clazz = (Class) i.next();
                String location = clazz.getProtectionDomain().getCodeSource().getLocation().toString();
                stream.println(location + "\t" + clazz.getName()); //$NON-NLS-1$
            }
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
    
}
