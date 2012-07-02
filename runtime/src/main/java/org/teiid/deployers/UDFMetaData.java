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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.teiid.metadata.FunctionMethod;


public class UDFMetaData {
	protected HashMap<String, Collection <FunctionMethod>> methods = new HashMap<String, Collection<FunctionMethod>>();	
	private ClassLoader classLoader;
		
	public Map<String, Collection <FunctionMethod>> getFunctions(){
		return this.methods;
	}
	
	public void addFunctions(String name, Collection <FunctionMethod> funcs){
		if (funcs.isEmpty()) {
			return;
		}
		Collection <FunctionMethod> old = this.methods.put(name, funcs);
		if (old != null) {
			ArrayList<FunctionMethod> combined = new ArrayList<FunctionMethod>(old);
			combined.addAll(funcs);
			this.methods.put(name, combined);
		}
	}
	
	public void addFunctions(UDFMetaData funcs){
		for (Map.Entry<String, Collection<FunctionMethod>> entry : funcs.getFunctions().entrySet()) {
			addFunctions(entry.getKey(), entry.getValue());
		}
	}

	public void setFunctionClassLoader(ClassLoader functionClassLoader) {
		this.classLoader = functionClassLoader;
	}
	
	public ClassLoader getClassLoader() {
		return classLoader;
	}
}
