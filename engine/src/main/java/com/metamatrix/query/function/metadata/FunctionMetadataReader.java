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

package com.metamatrix.query.function.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

import com.metamatrix.internal.core.xml.JdomHelper;


/**
 * This class is reader for the UDF xmi file. This class reads a xmi file and loads the functions 
 */
public class FunctionMetadataReader {
	
	private static final String NOT_ALLOWED = "NOT_ALLOWED"; //$NON-NLS-1$
	private static final String ALLOWED = "ALLOWED"; //$NON-NLS-1$
	private static final String REQUIRED = "REQUIRED"; //$NON-NLS-1$
	private static final String RETURN_PARAMETER = "returnParameter"; //$NON-NLS-1$
	private static final String TYPE = "type"; //$NON-NLS-1$
	private static final String INPUT_PARAMETERS = "inputParameters"; //$NON-NLS-1$
	private static final String DETERMINISTIC = "deterministic"; //$NON-NLS-1$
	private static final String PUSH_DOWN = "pushDown"; //$NON-NLS-1$
	private static final String INVOCATION_METHOD = "invocationMethod"; //$NON-NLS-1$
	private static final String INVOCATION_CLASS = "invocationClass"; //$NON-NLS-1$
	private static final String CATEGORY = "category"; //$NON-NLS-1$
	private static final String DESCRIPTION = "description"; //$NON-NLS-1$
	private static final String NAME = "name"; //$NON-NLS-1$
	private static final String SCALAR_FUNCTION = "ScalarFunction"; //$NON-NLS-1$
	private static final String NS_MMFUNCTION = "mmfunction"; //$NON-NLS-1$

	public static List<FunctionMethod> loadFunctionMethods(InputStream source) throws IOException {
		try {
			List<FunctionMethod> methods = new ArrayList<FunctionMethod>();
			
			Document doc = JdomHelper.buildDocument(source);
			Element rootElement = doc.getRootElement();
			
			// read the xmi file and load the functions
			Namespace ns = rootElement.getNamespace(NS_MMFUNCTION);			
			List<Element> functionElements = rootElement.getChildren(SCALAR_FUNCTION, ns);
			for (Element functionElement:functionElements) {
				
				String name = functionElement.getAttributeValue(NAME);
				String description = functionElement.getAttributeValue(DESCRIPTION);
				String category = functionElement.getAttributeValue(CATEGORY);
				String invocationClass = functionElement.getAttributeValue(INVOCATION_CLASS);
				String invocationMethod = functionElement.getAttributeValue(INVOCATION_METHOD);
				int pushdown= decodePushDownType(functionElement.getAttributeValue(PUSH_DOWN));
				boolean deterministic = Boolean.parseBoolean(functionElement.getAttributeValue(DETERMINISTIC));

				// read input parameters
				List<FunctionParameter> inParamters = new ArrayList<FunctionParameter>();
				List<Element> inputParameterElements = functionElement.getChildren(INPUT_PARAMETERS);
				for (Element inputElement:inputParameterElements) {
					inParamters.add(new FunctionParameter(inputElement.getAttributeValue(NAME), inputElement.getAttributeValue(TYPE), inputElement.getAttributeValue(DESCRIPTION)));
				}
				
				// read return for the function
				FunctionParameter returnParameter = null;
				Element returnElement = functionElement.getChild(RETURN_PARAMETER);
				if (returnElement != null) {
					returnParameter = new FunctionParameter(FunctionParameter.OUTPUT_PARAMETER_NAME,returnElement.getAttributeValue(TYPE), returnElement.getAttributeValue(DESCRIPTION));
				}
				
				FunctionMethod function = new FunctionMethod(
						name,
						description,
						category,
						pushdown,
						invocationClass,
						invocationMethod,
						inParamters.toArray(new FunctionParameter[inParamters.size()]),
						returnParameter,
						true,
						deterministic ? FunctionMethod.DETERMINISTIC: FunctionMethod.NONDETERMINISTIC);
				
				methods.add(function);
			}
			return methods;
		} catch (JDOMException e) {
			IOException ex = new IOException();
			ex.initCause(e);
			throw ex;
		}
	}

    private static int decodePushDownType(String pushdown) {
		if (pushdown != null) {
			if (pushdown.equals(REQUIRED)) {
				return FunctionMethod.CAN_PUSHDOWN;
			} else if (pushdown.equals(ALLOWED)) {
				return FunctionMethod.MUST_PUSHDOWN;
			} else if (pushdown.equals(NOT_ALLOWED)) {
				return FunctionMethod.CANNOT_PUSHDOWN;
			}
		}
		return FunctionMethod.CAN_PUSHDOWN;
	}

}
