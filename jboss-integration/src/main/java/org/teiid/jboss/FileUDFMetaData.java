/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
