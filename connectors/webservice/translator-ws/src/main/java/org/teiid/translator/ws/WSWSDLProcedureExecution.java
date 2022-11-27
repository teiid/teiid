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

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.stax.StAXSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.handler.MessageContext;

import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.util.StAXSQLXML;
import org.teiid.util.WSUtil;

/**
 * A WSDL soap call executor
 */
public class WSWSDLProcedureExecution implements ProcedureExecution {

    RuntimeMetadata metadata;
    ExecutionContext context;
    private Call procedure;
    private StAXSource returnValue;
    private WSConnection conn;
    private WSExecutionFactory executionFactory;

    public WSWSDLProcedureExecution(Call procedure, RuntimeMetadata metadata, ExecutionContext context, WSExecutionFactory executionFactory, WSConnection conn) {
        this.metadata = metadata;
        this.context = context;
        this.procedure = procedure;
        this.conn = conn;
        this.executionFactory = executionFactory;
    }

    public void execute() throws TranslatorException {
        List<Argument> arguments = this.procedure.getArguments();

        XMLType docObject = (XMLType)arguments.get(0).getArgumentValue().getValue();
        StAXSource source = null;
        try {
            source = convertToSource(docObject);

            Dispatch<StAXSource> dispatch = conn.createDispatch(StAXSource.class, executionFactory.getDefaultServiceMode());
            String operation = this.procedure.getProcedureName();
            if (this.procedure.getMetadataObject() != null && this.procedure.getMetadataObject().getNameInSource() != null) {
                operation = this.procedure.getMetadataObject().getNameInSource();
            }
            QName opQName = new QName(conn.getServiceQName().getNamespaceURI(), operation);
            dispatch.getRequestContext().put(MessageContext.WSDL_OPERATION, opQName);

            if (source == null) {
                // JBoss Native DispatchImpl throws exception when the source is null
                source = new StAXSource(XMLType.getXmlInputFactory().createXMLEventReader(new StringReader("<none/>"))); //$NON-NLS-1$
            }
            this.returnValue = dispatch.invoke(source);
        } catch (SQLException e) {
            throw new TranslatorException(e);
        } catch (WebServiceException e) {
            throw new TranslatorException(e);
        } catch (XMLStreamException e) {
            throw new TranslatorException(e);
        } catch (IOException e) {
            throw new TranslatorException(e);
        } finally {
            WSUtil.closeSource(source);
        }
    }

    private StAXSource convertToSource(SQLXML xml) throws SQLException {
        if (xml == null) {
            return null;
        }
        return xml.getSource(StAXSource.class);
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        return null;
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        Object result = returnValue;
        if (returnValue != null && procedure.getArguments().size() > 1 && Boolean.TRUE.equals(procedure.getArguments().get(1).getArgumentValue().getValue())) {
            SQLXMLImpl sqlXml = new StAXSQLXML(returnValue);
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
