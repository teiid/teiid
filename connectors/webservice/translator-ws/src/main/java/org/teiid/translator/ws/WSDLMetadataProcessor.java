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
package org.teiid.translator.ws;

import java.util.List;
import java.util.Map;

import javax.wsdl.*;
import javax.wsdl.extensions.ExtensibilityElement;
import javax.wsdl.extensions.http.HTTPBinding;
import javax.wsdl.extensions.soap.SOAPBinding;
import javax.wsdl.extensions.soap.SOAPOperation;
import javax.wsdl.extensions.soap12.SOAP12Binding;
import javax.wsdl.extensions.soap12.SOAP12Operation;
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
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.ws.WSExecutionFactory.Event;

public class WSDLMetadataProcessor implements MetadataProcessor<WSConnection> {

    private Definition definition;
    private boolean importWSDL = true;

    public WSDLMetadataProcessor() {

    }

    @Override
    public void process(MetadataFactory mf, WSConnection connection)
            throws TranslatorException {
        if (this.importWSDL && connection == null) {
            throw new TranslatorException(WSExecutionFactory.UTIL.gs(Event.TEIID15007, WSExecutionFactory.UTIL.gs(Event.TEIID15007)));
        }
        if (!importWSDL || connection.getWsdl() == null) {
            return;
        }
        String wsdl = connection.getWsdl().toString();
        try {
            WSDLFactory wsdlFactory = WSDLFactory.newInstance();
            WSDLReader reader = wsdlFactory.newWSDLReader();
            this.definition = reader.readWSDL(wsdl);
        } catch (WSDLException e) {
            throw new TranslatorException(e);
        }
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
        String outXML = "response"; //$NON-NLS-1$
        Output output = operation.getOutput();
        if (output != null) {
            Message message = output.getMessage();
            if (message != null) {
                outXML = message.getQName().getLocalPart();
            }
        }

        ExtensibilityElement operationExtension = getExtensibilityElement(bindingOperation.getExtensibilityElements(), "operation"); //$NON-NLS-1$
        if (!(operationExtension instanceof SOAPOperation) && !(operationExtension instanceof SOAP12Operation)) {
            return;
        }
        if (operationExtension instanceof SOAPOperation) {
            // soap:operation
            SOAPOperation soapOperation = (SOAPOperation) operationExtension;
            String style = soapOperation.getStyle();
            if (style.equalsIgnoreCase("rpc")) { //$NON-NLS-1$
                LogManager.logInfo(LogConstants.CTX_CONNECTOR,
                        WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15004, operation.getName()));
                return;
            }
        } else if (operationExtension instanceof SOAP12Operation) {
            // soap:operation
            SOAP12Operation soapOperation = (SOAP12Operation) operationExtension;
            String style = soapOperation.getStyle();
            if (style.equalsIgnoreCase("rpc")) { //$NON-NLS-1$
                LogManager.logInfo(LogConstants.CTX_CONNECTOR,
                        WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15004, operation.getName()));
                return;
            }
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

    @TranslatorProperty(display="Import WSDL", category=PropertyType.IMPORT, description="true to import WSDL for the metadata.")
    public boolean isImportWSDL() {
        return this.importWSDL;
    }

    public void setImportWSDL(boolean bool) {
        this.importWSDL = bool;
    }

}
