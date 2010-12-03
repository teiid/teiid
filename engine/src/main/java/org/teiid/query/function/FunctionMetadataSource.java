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

package org.teiid.query.function;

import java.util.Collection;

import org.teiid.metadata.FunctionMethod;


/**
 * A FunctionMetadataSource represents a source of function metadata for
 * the function library.  A FunctionMetadataSource needs to know how to 
 * return a collection of all the function signatures it knows about.
 */
public interface FunctionMetadataSource {

    /**
     * This method requests that the source return all 
     * {@link FunctionMethod}s
     * the source knows about.  This can occur in several situations - 
     * on initial registration with the FunctionLibraryManager, on a 
     * general reload, etc.  This may be called multiple times and should
     * always return the newest information available.
     * @return Collection of FunctionMethod objects
     */
    Collection<FunctionMethod> getFunctionMethods();
    
    /**
     * This method determines where the invocation classes specified in the 
     * function metadata are actually retrieved from.  
     * @param className Name of class
     * @return Class reference  
     * @throws ClassNotFoundException If class could not be found
     */
    Class getInvocationClass(String className) throws ClassNotFoundException;    
}
