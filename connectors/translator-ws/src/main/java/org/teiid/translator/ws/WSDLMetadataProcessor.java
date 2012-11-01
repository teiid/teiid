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
package org.teiid.translator.ws;

import java.util.List;
import java.util.Map;

import javax.wsdl.*;
import javax.wsdl.extensions.ExtensibilityElement;
import javax.wsdl.extensions.http.HTTPBinding;
import javax.wsdl.extensions.soap.SOAPBinding;
import javax.wsdl.extensions.soap.SOAPOperation;
import javax.wsdl.extensions.soap12.SOAP12Binding;
import javax.wsdl.factory.WSDLFactory;
import javax.wsdl.xml.WSDLReader;
import javax.xml.namespace.QName;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.WSConnection;

public class WSDLMetadataProcessor {
	
	private Definition definition;
	
	public WSDLMetadataProcessor(String wsdl) throws TranslatorException {
		try {
			WSDLFactory wsdlFactory = WSDLFactory.newInstance();
			WSDLReader reader = wsdlFactory.newWSDLReader();
			this.definition = reader.readWSDL(wsdl);
		} catch (WSDLException e) {
			throw new TranslatorException(e);
		}
	}

	public void getMetadata(MetadataFactory mf, WSConnection connection) throws TranslatorException {
		Map<QName, Service> services = this.definition.getServices();
		if (services == null || services.isEmpty()) {
			throw new TranslatorException(WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15001, connection.getServiceQName()));
		}
		Service service = services.get(connection.getServiceQName());
		
		if (service == null) {
			throw new TranslatorException(WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15001, connection.getServiceQName()));
		}
		
		Map<String, Port> ports = service.getPorts();
		Port port = ports.get(connection.getPortQName().getLocalPart());
		if (port == null) {
			throw new TranslatorException(WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15002, connection.getPortQName(), connection.getServiceQName()));
		}
		getPortMetadata(mf, port);
	}
	
	private void getPortMetadata(MetadataFactory mf, Port port) throws TranslatorException {
		Binding binding = port.getBinding();
		List<BindingOperation> operations = binding.getBindingOperations();
		if (operations == null || operations.isEmpty()) {
			return;
		}
		
		WSExecutionFactory.Binding executionBinding = extractExecutionBinding(binding);
		if (executionBinding == WSExecutionFactory.Binding.SOAP11 || executionBinding == WSExecutionFactory.Binding.SOAP12) {
			for (BindingOperation bindingOperation:operations) {
				buildSoapOperation(mf, bindingOperation);		
			}
		}
	}

	private WSExecutionFactory.Binding extractExecutionBinding(Binding binding) throws TranslatorException {
		WSExecutionFactory.Binding executionBinding = WSExecutionFactory.Binding.SOAP11;
		ExtensibilityElement bindingExtension = getExtensibilityElement(binding.getExtensibilityElements(), "binding"); //$NON-NLS-1$		
        if(bindingExtension instanceof SOAPBinding) {
        	executionBinding = WSExecutionFactory.Binding.SOAP11;
        }
        else if (bindingExtension instanceof SOAP12Binding) {
        	executionBinding = WSExecutionFactory.Binding.SOAP12;
        }
        else if (bindingExtension instanceof HTTPBinding) {
        	executionBinding = WSExecutionFactory.Binding.HTTP;
        }
        else {
        	throw new TranslatorException(WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15003));
        }
        return executionBinding;
	}
	
	private void buildSoapOperation(MetadataFactory mf,
			BindingOperation bindingOperation) {
		
		Operation operation = bindingOperation.getOperation();
				
		// add input
		String inputXML = null;
		Input input = operation.getInput();
		if (input != null) {
			Message message = input.getMessage();
			if (message != null) {
				inputXML = message.getQName().getLocalPart();
			}
		}

		// add output
		String outXML = null;
		Output output = operation.getOutput();
		if (output != null) {
			Message message = output.getMessage();
			if (message != null) {
				outXML = message.getQName().getLocalPart();
			}
		}
		
		ExtensibilityElement operationExtension = getExtensibilityElement(bindingOperation.getExtensibilityElements(), "operation"); //$NON-NLS-1$
		if (!(operationExtension instanceof SOAPOperation)) {
			return;
		}
		// soap:operation
		SOAPOperation soapOperation = (SOAPOperation) operationExtension;
		String style = soapOperation.getStyle();
		if (style.equalsIgnoreCase("rpc")) { //$NON-NLS-1$
			LogManager.logInfo(LogConstants.CTX_CONNECTOR, WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15004, operation.getName()));
			return;
		}

		Procedure procedure = mf.addProcedure(operation.getName());
		procedure.setVirtual(false);
		procedure.setNameInSource(operation.getName());
		
		mf.addProcedureParameter(inputXML, TypeFacility.RUNTIME_NAMES.XML, Type.In, procedure);
		
		ProcedureParameter param = mf.addProcedureParameter("stream", TypeFacility.RUNTIME_NAMES.BOOLEAN, Type.In, procedure); //$NON-NLS-1$
		param.setAnnotation("If the result should be streamed."); //$NON-NLS-1$
		param.setNullType(NullType.Nullable);
		param.setDefaultValue("false"); //$NON-NLS-1$
		
		mf.addProcedureParameter(outXML, TypeFacility.RUNTIME_NAMES.XML, Type.ReturnValue, procedure);
	}
	
	private ExtensibilityElement getExtensibilityElement(List<ExtensibilityElement> elements, String type) {
		if (elements == null || elements.isEmpty()) {
			return null;
		}
		for (ExtensibilityElement element: elements) {
			if (element.getElementType().getLocalPart().equalsIgnoreCase(type)) {
				return element;
			}
		}
		return null;
	}	
}
