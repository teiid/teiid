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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.handler.MessageContext;

import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.ws.WSExecutionFactory.Binding;
import org.teiid.util.StAXSQLXML;
import org.teiid.util.WSUtil;

/**
 * A soap call executor - handles all styles doc/literal, rpc/encoded etc.
 */
public class WSProcedureExecution implements ProcedureExecution {

    RuntimeMetadata metadata;
    ExecutionContext context;
    private Call procedure;
    private Source returnValue;
    private WSConnection conn;
    private WSExecutionFactory executionFactory;

    public WSProcedureExecution(Call procedure, RuntimeMetadata metadata, ExecutionContext context, WSExecutionFactory executionFactory, WSConnection conn) {
        this.metadata = metadata;
        this.context = context;
        this.procedure = procedure;
        this.conn = conn;
        this.executionFactory = executionFactory;
    }

    @SuppressWarnings("unchecked")
    public void execute() throws TranslatorException {
        List<Argument> arguments = this.procedure.getArguments();

        String style = (String)arguments.get(0).getArgumentValue().getValue();
        String action = (String)arguments.get(1).getArgumentValue().getValue();
        XMLType docObject = (XMLType)arguments.get(2).getArgumentValue().getValue();
        Source source = null;
        try {
            Class type = StAXSource.class;
            if (executionFactory.getDefaultServiceMode() == Mode.MESSAGE) {
                type = DOMSource.class;
            }

            source = convertToSource(type, docObject);
            String endpoint = (String)arguments.get(3).getArgumentValue().getValue();

            if (style == null) {
                style = executionFactory.getDefaultBinding().getBindingId();
            } else {
                try {
                    style = Binding.valueOf(style.toUpperCase()).getBindingId();
                } catch (IllegalArgumentException e) {
                    throw new TranslatorException(WSExecutionFactory.UTIL.getString("invalid_invocation", Arrays.toString(Binding.values()))); //$NON-NLS-1$
                }
            }

            Dispatch dispatch = conn.createDispatch(style, endpoint, type, executionFactory.getDefaultServiceMode());

            if (Binding.HTTP.getBindingId().equals(style)) {
                if (action == null) {
                    action = "POST"; //$NON-NLS-1$
                }
                dispatch.getRequestContext().put(MessageContext.HTTP_REQUEST_METHOD, action);
                if (source != null && !"POST".equalsIgnoreCase(action)) { //$NON-NLS-1$
                    if (this.executionFactory.getXMLParamName() == null) {
                        throw new WebServiceException(WSExecutionFactory.UTIL.getString("http_usage_error")); //$NON-NLS-1$
                    }
                    try {
                        Transformer t = TransformerFactory.newInstance().newTransformer();
                        StringWriter writer = new StringWriter();
                        //TODO: prevent this from being too large
                        t.transform(source, new StreamResult(writer));
                        String param = WSUtil.httpURLEncode(this.executionFactory.getXMLParamName())+"="+WSUtil.httpURLEncode(writer.toString()); //$NON-NLS-1$
                        endpoint = WSUtil.appendQueryString(endpoint, param);
                    } catch (TransformerException e) {
                        throw new WebServiceException(e);
                    }
                }
            } else {
                if (action != null) {
                    dispatch.getRequestContext().put(Dispatch.SOAPACTION_USE_PROPERTY, Boolean.TRUE);
                    dispatch.getRequestContext().put(Dispatch.SOAPACTION_URI_PROPERTY, action);
                }
            }

            if (source == null) {
                // JBoss Native DispatchImpl throws exception when the source is null
                source = new StAXSource(XMLType.getXmlInputFactory().createXMLEventReader(new StringReader("<none/>"))); //$NON-NLS-1$
            }
            this.returnValue = (Source) dispatch.invoke(source);
        } catch (SQLException e) {
            throw new TranslatorException(e);
        } catch (WebServiceException e) {
            throw new TranslatorException(e);
        } catch (XMLStreamException e) {
            throw new TranslatorException(e);
        } finally {
            WSUtil.closeSource(source);
        }
    }

    private Source convertToSource(Class<? extends Source> T, SQLXML xml) throws SQLException {
        if (xml == null) {
            return null;
        }
        return xml.getSource(T);
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        return null;
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        Object result = returnValue;
        if (returnValue != null && (returnValue instanceof StAXSource) && procedure.getArguments().size() > 4
                && procedure.getArguments().get(4).getDirection() == Direction.IN
                && Boolean.TRUE.equals(procedure.getArguments().get(4).getArgumentValue().getValue())) {
            SQLXMLImpl sqlXml = new StAXSQLXML((StAXSource)returnValue);
            XMLType xml = new XMLType(sqlXml);
            xml.setType(Type.DOCUMENT);
            result = xml;
        } else if (returnValue != null && returnValue instanceof DOMSource){
            final DOMSource xmlSource = (DOMSource) returnValue;
            SQLXMLImpl sqlXml = new SQLXMLImpl(new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    Result outputTarget = new StreamResult(outputStream);
                    try {
                        TransformerFactory.newInstance().newTransformer()
                                .transform(xmlSource, outputTarget);
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                    return new ByteArrayInputStream(outputStream.toByteArray());
                }
            });
            XMLType xml = new XMLType(sqlXml);
            xml.setType(Type.DOCUMENT);
            result = xml;
        }
        return Arrays.asList(result);
    }

    public void close() {

    }

    public void cancel() throws TranslatorException {
        // no-op
    }
}
