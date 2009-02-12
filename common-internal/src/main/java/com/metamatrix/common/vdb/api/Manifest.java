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

package com.metamatrix.common.vdb.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

import com.metamatrix.common.xml.XMLReaderWriter;
import com.metamatrix.common.xml.XMLReaderWriterImpl;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.vdb.ModelType;
import com.metamatrix.vdb.runtime.BasicModelInfo;
import com.metamatrix.vdb.runtime.BasicVDBDefn;


/**
 * VDB Manifest file; right now this is defined as EMF resource in the vdb.edit code, 
 * so to break the dependency from EMF we have defined as the simple XML type here. This
 * will defined as simple xml in future then we can remove this. There is defect logged on this
 * in JIRA
 */
class Manifest {
	private static final String MODELS = "models"; //$NON-NLS-1$
	private static final String ACCESSIBILITY = "accessibility"; //$NON-NLS-1$
	private static final String PRIMARY_METAMODEL_URI = "primaryMetamodelUri"; //$NON-NLS-1$
	private static final String UUID = "uuid"; //$NON-NLS-1$
	private static final String NS = "vdb"; //$NON-NLS-1$
	private static final String MESSAGE = "message"; //$NON-NLS-1$
	private static final String ERROR = "ERROR"; //$NON-NLS-1$
	private static final String SEVERITY = "severity"; //$NON-NLS-1$
	private static String NAME = "name"; //$NON-NLS-1$
	private static String MODEL_PATH = "path";//$NON-NLS-1$
	private static String MODEL_TYPE = "modelType";//$NON-NLS-1$
	private static String VDB_ELEMENT = "VirtualDatabase";//$NON-NLS-1$
	private static String DESCRIPTION = "description";//$NON-NLS-1$
	private static String MARKERS = "markers";//$NON-NLS-1$
	
	List<String> validityErrors = new ArrayList<String>();
	BasicVDBDefn vdb;

	public void load(InputStream in) throws IOException {
    	try {
			XMLReaderWriter reader = new XMLReaderWriterImpl();
			Document doc = reader.readDocument(in);
			Element root = doc.getRootElement();
			
			Namespace ns= root.getNamespace(NS);
			Element vdbElement = root.getChild(VDB_ELEMENT, ns);
			if (vdbElement != null) {
				// Build VDB info
				vdb = new BasicVDBDefn(vdbElement.getAttributeValue(NAME));
				vdb.setDescription(vdbElement.getAttributeValue(DESCRIPTION));
				vdb.setVersion("1"); //$NON-NLS-1$
				
				// build the models
				List<Element> modelElements = vdbElement.getChildren(MODELS);
				for(Element modelElement:modelElements) {
					
					BasicModelInfo model = new BasicModelInfo(StringUtil.getFirstToken(modelElement.getAttributeValue(NAME), ".")); //$NON-NLS-1$
					model.setModelType(ModelType.parseString(modelElement.getAttributeValue(MODEL_TYPE)));
					model.setPath(modelElement.getAttributeValue(MODEL_PATH));
					model.setUuid(modelElement.getAttributeValue(UUID));
					model.setModelURI(modelElement.getAttributeValue(PRIMARY_METAMODEL_URI));
					
					String visibility = modelElement.getAttributeValue(ACCESSIBILITY);
					if (visibility != null) {
						model.setVisibility(ModelInfo.PUBLIC_VISIBILITY.equals(visibility)?ModelInfo.PUBLIC:ModelInfo.PRIVATE);
					}
					
					List<Element> markers = modelElement.getChildren(MARKERS);
					for(Element marker:markers) {
						String severity = marker.getAttributeValue(SEVERITY);
						if (severity.equals(ERROR)) {
							validityErrors.add(marker.getAttributeValue(MESSAGE));
						}
					}
					
					this.vdb.addModelInfo(model);
				}
			}
		} catch (JDOMException e) {
			throw new IOException("Failed to read the VDB-Manifest file"); //$NON-NLS-1$
		}
	}
	
	public String[] getValidityErrors() {
		if (!validityErrors.isEmpty()) {
			return validityErrors.toArray(new String[validityErrors.size()]);
		}
		return null;
	}
	
	public BasicVDBDefn getVDB() {
		return this.vdb;
	}

}
