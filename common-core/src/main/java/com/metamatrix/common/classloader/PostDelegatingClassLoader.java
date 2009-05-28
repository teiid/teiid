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

import java.net.URL;
import java.net.URLStreamHandlerFactory;

/**
 * This Class circumvents the 
 * java ClassLoader delegation model.  This ClassLoader
 * will first look in it's own store of classes, and only
 * then check it's parent ClassLoader, which is the reverse
 * of the delegation model.
 */
public class PostDelegatingClassLoader extends URLFilteringClassLoader {

    public PostDelegatingClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }
    
    public PostDelegatingClassLoader(URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory) {
        super(urls, parent, factory);
    }
    

    public PostDelegatingClassLoader(URL[] urls) {
        super(urls);
    }
    

    /**
     * By overriding this method, this Class circumvents the 
     * java ClassLoader delegation model.  This ClassLoader
     * will first look in it's own store of classes, and only
     * then check it's parent ClassLoader, which is the reverse
     * of the delegation model.
     * <p> 
     * @param name The name of the class to load 
     * @return Class loaded Class object
     * @see java.lang.ClassLoader#loadClass(java.lang.String)
     */    
    public synchronized Class loadClass(String name) throws ClassNotFoundException {
        
    	if (name.startsWith("javax.") || name.startsWith("java.")) { //$NON-NLS-1$ //$NON-NLS-2$
			return super.loadClass(name);
    	}
    	
        Class loadedClass = this.findLoadedClass(name);
        if (loadedClass != null) {
            return loadedClass;
        }

        // class not in cache
        try {
            loadedClass = super.findClass(name);
        } catch (ClassNotFoundException e) {
            // ignore, check parent ClassLoader
        } catch (SecurityException e) {
            // ignore, check parent ClassLoader
            /*
             * ClassLoader.defineClass throws a SecurityException if this
             * ClassLoader attempts to load a class under the "java."
             * package hierarchy.
             */
        }
         
        // if class not found, delegate to parent
        if(loadedClass == null) {
           // Will throw ClassNotFoundException if not found in parent
           loadedClass = this.getParent().loadClass(name);
        }
        
    
        return loadedClass;
    }
        
    
    /**
     * By overriding this method, this Class circumvents the 
     * java ClassLoader delegation model.  This ClassLoader
     * will first look in it's own store of resources, and only
     * then check it's parent ClassLoader, which is the reverse
     * of the delegation model.
     * @param name The name of the resource to load 
     * @return URL of resource
     * @see java.lang.ClassLoader#getResource(java.lang.String)
     */
    public URL getResource(String name) {
        URL url = super.findResource(name);
        if (url == null){
            url = this.getParent().getResource(name);
        }
        return url; 
    }        
}
