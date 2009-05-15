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

package com.metamatrix.query.function;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;

import com.metamatrix.common.classloader.PostDelegatingClassLoader;
import com.metamatrix.common.protocol.MetaMatrixURLStreamHandlerFactory;
import com.metamatrix.query.function.metadata.FunctionMetadataReader;
import com.metamatrix.query.function.metadata.FunctionMethod;


public class UDFSource implements FunctionMetadataSource {
	
    private URL[] classpath = null;
    private ClassLoader classLoader = null;
    private Collection <FunctionMethod> methods = null;
    
    public UDFSource(URL url) throws IOException {
    	loadFunctions(url.openStream());
    }    
    
    public UDFSource(URL url, URL[] classpath) throws IOException{
        this.classpath = classpath;
        loadFunctions(url.openStream());
    }
    
    public UDFSource(InputStream udfStream, URL[] classpath) throws IOException {
        this.classpath = classpath;
        loadFunctions(udfStream);
    }
    
    public UDFSource(InputStream udfStream, ClassLoader classloader) throws IOException {
        this.classLoader = classloader;
        loadFunctions(udfStream);
    }    
    
    
    public Collection getFunctionMethods() {
        return this.methods;
    }

    public Class getInvocationClass(String className) throws ClassNotFoundException {
        // If no classpath is specified then use the default classpath
        if (this.classLoader == null && (classpath == null || classpath.length == 0)) {
            return Class.forName(className);
        }
        
        // If the class loader is not created for the UDF functions then create 
        // one and cache it.
        if (classLoader == null) {
            classLoader = new PostDelegatingClassLoader(this.classpath, Thread.currentThread().getContextClassLoader(), new MetaMatrixURLStreamHandlerFactory());                        
        }
        
        return classLoader.loadClass(className);
    }

    public void loadFunctions(InputStream in) throws IOException{
        methods = FunctionMetadataReader.loadFunctionMethods(in);
    }  
}
