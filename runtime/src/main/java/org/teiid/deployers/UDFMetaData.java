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
import java.util.Map;
import java.util.TreeMap;

import org.teiid.metadata.FunctionMethod;
import org.teiid.query.function.UDFSource;


public class UDFMetaData {
	protected TreeMap<String, UDFSource> methods = new TreeMap<String, UDFSource>(String.CASE_INSENSITIVE_ORDER);	
	private ClassLoader classLoader;
	
	public Map<String, UDFSource> getFunctions(){
		return this.methods;
	}

	public void addFunctions(String name, Collection <FunctionMethod> funcs){
		if (funcs.isEmpty()) {
			return;
		}
		UDFSource udfSource = this.methods.get(name);
		if (udfSource != null) {
			//this is ambiguous about as to what classloader to use, but we assume the first is good and that the user will have set 
			//the Java method if that's not the case
			ArrayList<FunctionMethod> allMethods = new ArrayList<FunctionMethod>(udfSource.getFunctionMethods());
			allMethods.addAll(funcs);
			ClassLoader cl = udfSource.getClassLoader();
			udfSource = new UDFSource(allMethods);
			udfSource.setClassLoader(cl);
		} else {
			udfSource = new UDFSource(funcs);
			udfSource.setClassLoader(classLoader);
		}
		this.methods.put(name, udfSource);
	}
	
	public void addFunctions(UDFMetaData funcs){
		this.methods.putAll(funcs.methods);
		this.classLoader = funcs.classLoader;
	}

	public void setFunctionClassLoader(ClassLoader functionClassLoader) {
		for (UDFSource udf : methods.values()) {
			udf.setClassLoader(functionClassLoader);
		}
		this.classLoader = functionClassLoader;
	}
	
	public ClassLoader getClassLoader() {
		return classLoader;
	}
}
