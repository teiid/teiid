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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.virtual.VirtualFile;
import org.teiid.query.function.metadata.FunctionMetadataReader;
import org.teiid.query.function.metadata.FunctionMethod;
import org.teiid.runtime.RuntimePlugin;


@ManagementObject
public class UDFMetaData {
	private Collection <FunctionMethod> methods = new ArrayList<FunctionMethod>();
	
	private HashMap<String, VirtualFile> files = new HashMap<String, VirtualFile>();
	
	public void addModelFile(VirtualFile file) {
		this.files.put(file.getPathName(), file);
	}
	
	
	void buildFunctionModelFile(String name) throws IOException {
		VirtualFile file = this.files.get(name);
		if (file != null) {
			this.methods.addAll(FunctionMetadataReader.loadFunctionMethods(file.openStream()));
		}
		else {
			throw new IOException(RuntimePlugin.Util.getString("udf_model_not_found", name)); //$NON-NLS-1$
		}
	}
	
	public Collection <FunctionMethod> getFunctions(){
		return this.methods;
	}
	
	public void addFunctions(Collection <FunctionMethod> funcs){
		this.methods.addAll(funcs);
	}
}
