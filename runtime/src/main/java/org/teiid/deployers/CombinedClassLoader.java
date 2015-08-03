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

package org.teiid.deployers;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Vector;

/** A Classloader that takes in two Classloaders to delegate to */
public class CombinedClassLoader extends ClassLoader {
    private ClassLoader[] toSearch;

    public CombinedClassLoader(ClassLoader parent, ClassLoader... toSearch){
    	super(parent);
        this.toSearch = toSearch;
    }
    
    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
    	for (ClassLoader cl : toSearch) {
    		if (cl == null) {
    			continue;
    		}
    		try {
    			return cl.loadClass(name);
    		} catch (ClassNotFoundException e) {
    			
    		}
		}
    	return super.loadClass(name);
    }
    
    @Override
    protected URL findResource(String name) {
    	for (ClassLoader cl : toSearch) {
    		if (cl == null) {
    			continue;
    		}
			URL url = cl.getResource(name);
			if (url != null) {
				return url;
			}
    	}
    	return super.getResource(name);
    }
    
    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
    	Vector<URL> result = new Vector<URL>();
    	for (ClassLoader cl : toSearch) {
    		if (cl == null) {
    			continue;
    		}
    		Enumeration<URL> url = cl.getResources(name);
    		result.addAll(Collections.list(url));
    	}
    	return result.elements();
    }
    
}