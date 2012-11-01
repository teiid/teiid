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

import javax.wsdl.Binding;
import javax.wsdl.BindingInput;
import javax.wsdl.BindingOperation;
import javax.wsdl.Definition;
import javax.wsdl.Input;
import javax.wsdl.Message;
import javax.wsdl.Operation;
import javax.wsdl.Output;
import javax.wsdl.Port;
import javax.wsdl.Service;
import javax.wsdl.WSDLException;
import javax.wsdl.extensions.ExtensibilityElement;
import javax.wsdl.extensions.http.HTTPAddress;
import javax.wsdl.extensions.http.HTTPBinding;
import javax.wsdl.extensions.soap.SOAPAddress;
import javax.wsdl.extensions.soap.SOAPBinding;
import javax.wsdl.extensions.soap.SOAPBody;
import javax.wsdl.extensions.soap.SOAPOperation;
import javax.wsdl.extensions.soap12.SOAP12Binding;
import javax.wsdl.factory.WSDLFactory;
import javax.wsdl.xml.WSDLReader;
import javax.xml.namespace.QName;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.WSConnection;

public class WSDLMetadataProcessor {
	private final static String DEFAULT_SOAP_ENCODING = "http://schemas.xmlsoap.org/soap/encoding/"; //$NON-NLS-1$
	static final String BINDING = "binding"; //$NON-NLS-1$
	static final String ACTION = "action"; //$NON-NLS-1$
	static final String ENDPOINT = "endpoint"; //$NON-NLS-1$
	static final String ENCODING = "encoding"; //$NON-NLS-1$
	static final String XML_PARAMETER = "xml-parameter"; //$NON-NLS-1$
	
	private Definition definition;
	private String serviceName;
	private String portName;
	
	
	public WSDLMetadataProcessor(String wsdl) throws TranslatorException {
		try {
			WSDLFactory wsdlFactory = WSDLFactory.newInstance();
			WSDLReader reader = wsdlFactory.newWSDLReader();
			this.definition = reader.readWSDL(wsdl);
		} catch (WSDLException e) {
			throw new TranslatorException(e);
		}
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getPortName() {
		return portName;
	}

	public void setPortName(String portName) {
		this.portName = portName;
	}
	
	public void getMetadata(MetadataFactory mf, @SuppressWarnings("unused") WSConnection connection) throws TranslatorException {
		Map<QName, Service> services = this.definition.getServices();
		if (services == null || services.isEmpty()) {
			return;
		}
		
		Service service = null;
		for (QName name:services.keySet()) {
			// if service name not defined then get the first one.
			if (this.serviceName == null) {
				service = services.get(name);
				break;
			}
			if (name.getLocalPart().equalsIgnoreCase(this.serviceName)) {
				service = services.get(name);
				break;
			}
		}
		
		if (service == null) {
			throw new TranslatorException(WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15001, this.serviceName));
		}
		
		getServiceMetadata(mf,service);
	}
	
	private void getServiceMetadata(MetadataFactory mf, Service service) throws TranslatorException {
		Port port = null;
		Map<String, Port> ports = service.getPorts();
		for (String name:ports.keySet()) {
			if (this.portName == null) {
				port = ports.get(name);
				break;
			}
			if (name.equalsIgnoreCase(this.portName)) {
				port = ports.get(name);
				break;
			}
		}
		
		if (port == null) {
			throw new TranslatorException(WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15002, this.portName, this.serviceName));
		}
		getPortMetadata(mf, port);
	}
	
	private void getPortMetadata(MetadataFactory mf, Port port) throws TranslatorException {
		String address = null;
        
		ExtensibilityElement addressExtension = getExtensibilityElement(port.getExtensibilityElements(), "address"); //$NON-NLS-1$
        if(addressExtension instanceof SOAPAddress){
           address = ((SOAPAddress)addressExtension).getLocationURI();
        }
        else if (addressExtension instanceof HTTPAddress) {
        	address = ((HTTPAddress)addressExtension).getLocationURI();
        }
		
		Binding binding = port.getBinding();
		List<BindingOperation> operations = binding.getBindingOperations();
		if (operations == null || operations.isEmpty()) {
			return;
		}
		
		WSExecutionFactory.Binding executionBinding = extractExecutionBinding(binding);
		if (executionBinding == WSExecutionFactory.Binding.SOAP11 || executionBinding == WSExecutionFactory.Binding.SOAP12) {
			for (BindingOperation bindingOperation:operations) {
				buildSoapOperation(mf, bindingOperation, executionBinding, address);		
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
			BindingOperation bindingOperation,
			WSExecutionFactory.Binding binding, String endpoint) {
		
		Operation operation = bindingOperation.getOperation();
		BindingInput bindingInput = bindingOperation.getBindingInput();
				
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
		
		String encodingStyle = DEFAULT_SOAP_ENCODING;
		String action = null;
		ExtensibilityElement operationExtension = getExtensibilityElement(bindingOperation.getExtensibilityElements(), "operation"); //$NON-NLS-1$
		if (operationExtension instanceof SOAPOperation) {
			// soap:operation
			SOAPOperation soapOperation = (SOAPOperation) operationExtension;
			action = soapOperation.getSoapActionURI();
			String style = soapOperation.getStyle();
			if (style.equalsIgnoreCase("rpc")) { //$NON-NLS-1$
				LogManager.logInfo(LogConstants.CTX_CONNECTOR, WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15004, operation.getName()));
				return;
			}
			
			ExtensibilityElement bodyExtension = getExtensibilityElement(bindingInput.getExtensibilityElements(), "body"); //$NON-NLS-1$
			if (bodyExtension instanceof SOAPBody) {
				SOAPBody soapBody = (SOAPBody) bodyExtension;
				List styles = soapBody.getEncodingStyles();
				if (styles != null) {
					encodingStyle = styles.get(0).toString();
				}
			}
			
		}
		else {
			return;
		}

		Procedure procedure = mf.addProcedure(operation.getName());
		procedure.setVirtual(false);
		
		mf.addProcedureParameter(inputXML, TypeFacility.RUNTIME_NAMES.XML, Type.In, procedure);
		mf.addProcedureParameter(outXML, TypeFacility.RUNTIME_NAMES.XML, Type.ReturnValue, procedure);
		
		procedure.setProperty(MetadataFactory.WS_URI+ENCODING, encodingStyle);
		procedure.setProperty(MetadataFactory.WS_URI+BINDING, binding.name());
		procedure.setProperty(MetadataFactory.WS_URI+ACTION, action);
		if (endpoint != null) {
			procedure.setProperty(MetadataFactory.WS_URI+ENDPOINT, endpoint);
		}
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
