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
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.virtual.VirtualFile;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.metadata.FunctionMethod;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionMetadataReader;
import org.teiid.query.function.metadata.FunctionMetadataValidator;
import org.teiid.query.report.ActivityReport;
import org.teiid.query.report.ReportItem;
import org.teiid.runtime.RuntimePlugin;


@ManagementObject
public class UDFMetaData {
	private HashMap<String, Collection <FunctionMethod>> methods = new HashMap<String, Collection<FunctionMethod>>();
	private HashMap<String, VirtualFile> files = new HashMap<String, VirtualFile>();
	
	public void addModelFile(VirtualFile file) {
		this.files.put(file.getPathName(), file);
	}
	
	
	void buildFunctionModelFile(String name, String path) throws IOException, JAXBException, QueryMetadataException {
		for (String f:files.keySet()) {
			if (f.endsWith(path)) {
				path = f;
				break;
			}
		}
		VirtualFile file =this.files.get(path);
		if (file == null) {
			throw new IOException(RuntimePlugin.Util.getString("udf_model_not_found", name)); //$NON-NLS-1$
		}
		List<FunctionMethod> udfMethods = FunctionMetadataReader.loadFunctionMethods(file.openStream());
		ActivityReport<ReportItem> report = new ActivityReport<ReportItem>("UDF load"); //$NON-NLS-1$
		FunctionMetadataValidator.validateFunctionMethods(udfMethods,report);
		if(report.hasItems()) {
		    throw new QueryMetadataException(QueryPlugin.Util.getString("ERR.015.001.0005", report)); //$NON-NLS-1$
		}
		this.methods.put(name, udfMethods);
	}
	
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
}
