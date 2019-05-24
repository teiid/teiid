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

import java.util.Collections;
import java.util.List;

import org.teiid.resource.api.ConnectionFactory;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.http.HTTPBinding;
import javax.xml.ws.soap.SOAPBinding;

import org.teiid.core.BundleUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.Call;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;

@Translator(name="ws", description="A translator for making Web Service calls")
public class WSExecutionFactory extends ExecutionFactory<ConnectionFactory, WSConnection> {

    static final String INVOKE = "invoke"; //$NON-NLS-1$

    static final String INVOKE_HTTP = "invokeHttp"; //$NON-NLS-1$

    public enum Binding {
        HTTP(HTTPBinding.HTTP_BINDING),
        SOAP11(SOAPBinding.SOAP11HTTP_BINDING),
        SOAP12(SOAPBinding.SOAP12HTTP_BINDING);

        private String bindingId;

        private Binding(String bindingId) {
            this.bindingId = bindingId;
        }

        public String getBindingId() {
            return bindingId;
        }
    }

    public static BundleUtil UTIL = BundleUtil.getBundleUtil(WSExecutionFactory.class);
    public static enum Event implements BundleUtil.Event{
        TEIID15001,
        TEIID15002,
        TEIID15003,
        TEIID15004,
        TEIID15005,
        TEIID15006,
        TEIID15007
    }

    private Mode defaultServiceMode = Mode.PAYLOAD;
    private Binding defaultBinding = Binding.SOAP12;
    private String xmlParamName;

    public WSExecutionFactory() {
        setSourceRequiredForMetadata(false);
        setTransactionSupport(TransactionSupport.NONE);
    }

    @TranslatorProperty(description="Contols request/response message wrapping - set to MESSAGE for full control over SOAP messages.", display="Default Service FrameMode")
    public Mode getDefaultServiceMode() {
        return defaultServiceMode;
    }

    public void setDefaultServiceMode(Mode mode) {
        this.defaultServiceMode = mode;
    }

    @TranslatorProperty(description="Contols what SOAP or HTTP type of invocation will be used if none is specified.", display="Default Binding")
    public Binding getDefaultBinding() {
        return defaultBinding;
    }

    public void setDefaultBinding(Binding defaultInvocationType) {
        this.defaultBinding = defaultInvocationType;
    }

    @TranslatorProperty(description="Used with the HTTP binding (typically with the GET method) to indicate that the request document should be part of the query string.", display="XML Param Name")
    public String getXMLParamName() {
        return xmlParamName;
    }

    public void setXMLParamName(String xmlParamName) {
        this.xmlParamName = xmlParamName;
    }

    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, WSConnection connection)
            throws TranslatorException {
        if (command.getProcedureName().equalsIgnoreCase(INVOKE_HTTP)) {
            return new BinaryWSProcedureExecution(command, metadata, executionContext, this, connection);
        }
        if (command.getArguments().size() > 2 || command.getProcedureName().equalsIgnoreCase(INVOKE)) {
            return new WSProcedureExecution(command, metadata, executionContext, this, connection);
        }
        return new WSWSDLProcedureExecution(command, metadata, executionContext, this, connection);
    }

    @Override
    public final List<String> getSupportedFunctions() {
        return Collections.emptyList();
    }

    @Override
    public MetadataProcessor<WSConnection> getMetadataProcessor() {
        return new WSDLMetadataProcessor();
    }

    @Override
    public void getMetadata(MetadataFactory metadataFactory, WSConnection conn) throws TranslatorException {
        Procedure p = metadataFactory.addProcedure(INVOKE);
        p.setAnnotation("Invokes a webservice that returns an XML result"); //$NON-NLS-1$

        metadataFactory.addProcedureParameter("result", TypeFacility.RUNTIME_NAMES.XML, Type.ReturnValue, p); //$NON-NLS-1$

        ProcedureParameter param = metadataFactory.addProcedureParameter("binding", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("The invocation binding (HTTP, SOAP11, SOAP12).  May be set or allowed to default to null to use the default binding."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("action", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("With a SOAP invocation, action sets the SOAPAction.  With HTTP it sets the HTTP Method (GET, POST - default, etc.)."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        //can be one of string, xml, clob
        param = metadataFactory.addProcedureParameter("request", TypeFacility.RUNTIME_NAMES.XML, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("The XML document or root element that represents the request.  If the ExecutionFactory is configured in with a DefaultServiceMode of MESSAGE, then the SOAP request must contain the entire SOAP message."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("endpoint", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("The relative or abolute endpoint to use.  May be set or allowed to default to null to use the default endpoint address."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("stream", TypeFacility.RUNTIME_NAMES.BOOLEAN, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("If the result should be streamed."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);
        param.setDefaultValue("false"); //$NON-NLS-1$

        p = metadataFactory.addProcedure(INVOKE_HTTP);
        p.setAnnotation("Invokes a webservice that returns an binary result"); //$NON-NLS-1$

        metadataFactory.addProcedureParameter("result", TypeFacility.RUNTIME_NAMES.BLOB, Type.ReturnValue, p); //$NON-NLS-1$

        param = metadataFactory.addProcedureParameter("action", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Sets the HTTP Method (GET, POST - default, etc.)."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        //can be one of string, xml, clob
        param = metadataFactory.addProcedureParameter("request", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("The String, XML, BLOB, or CLOB value containing a payload (only for POST)."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("endpoint", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("The relative or abolute endpoint to use.  May be set or allowed to default to null to use the default endpoint address."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("stream", TypeFacility.RUNTIME_NAMES.BOOLEAN, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("If the result should be streamed."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);
        param.setDefaultValue("false"); //$NON-NLS-1$

        metadataFactory.addProcedureParameter("contentType", TypeFacility.RUNTIME_NAMES.STRING, Type.Out, p); //$NON-NLS-1$

        param = metadataFactory.addProcedureParameter("headers", TypeFacility.RUNTIME_NAMES.CLOB, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Headers to send"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        WSDLMetadataProcessor metadataProcessor = new WSDLMetadataProcessor();
        PropertiesUtils.setBeanProperties(metadataProcessor, metadataFactory.getModelProperties(), "importer"); //$NON-NLS-1$
        metadataProcessor.process(metadataFactory, conn);
    }

    @Override
    public boolean areLobsUsableAfterClose() {
        return true;
    }

}
