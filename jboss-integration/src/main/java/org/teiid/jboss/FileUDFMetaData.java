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

package org.teiid.jboss;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.jboss.vfs.VirtualFile;
import org.teiid.deployers.UDFMetaData;
import org.teiid.metadata.FunctionMethod;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.UDFSource;
import org.teiid.query.function.metadata.FunctionMetadataReader;
import org.teiid.query.function.metadata.FunctionMetadataValidator;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.runtime.RuntimePlugin;

public class FileUDFMetaData extends UDFMetaData {
	
	private HashMap<String, VirtualFile> files = new HashMap<String, VirtualFile>();
	
	public void addModelFile(VirtualFile file) {
		this.files.put(file.getPathName(), file);
	}
	
	
	public void buildFunctionModelFile(String name, String path) throws IOException, XMLStreamException {
		for (String f:files.keySet()) {
			if (f.endsWith(path)) {
				path = f;
				break;
			}
		}
		VirtualFile file =this.files.get(path);
		if (file == null) {
			throw new IOException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40075, name));
		}
		List<FunctionMethod> udfMethods = FunctionMetadataReader.loadFunctionMethods(file.openStream());
		ValidatorReport report = new ValidatorReport("UDF load"); //$NON-NLS-1$
		FunctionMetadataValidator.validateFunctionMethods(udfMethods,report);
		if(report.hasItems()) {
		    throw new IOException(QueryPlugin.Util.getString("ERR.015.001.0005", report)); //$NON-NLS-1$
		}
		this.methods.put(name, new UDFSource(udfMethods));
	}
	

}
