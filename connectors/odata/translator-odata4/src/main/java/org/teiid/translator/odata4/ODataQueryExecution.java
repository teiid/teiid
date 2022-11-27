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
package org.teiid.translator.odata4;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.language.AggregateFunction;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.odata4.ODataMetadataProcessor.ODataType;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;

public class ODataQueryExecution extends BaseQueryExecution implements ResultSetExecution {

    private ODataSQLVisitor visitor;
    private int countResponse = -1;
    private Class<?>[] expectedColumnTypes;
    private ODataResponse response;
    private boolean isCount;

    public ODataQueryExecution(ODataExecutionFactory translator,
            QueryExpression command, ExecutionContext executionContext,
            RuntimeMetadata metadata, WSConnection connection) throws TranslatorException {
        super(translator, executionContext, metadata, connection);

        this.visitor = new ODataSQLVisitor(this.translator, metadata);
        this.visitor.visitNode(command);
        if (command instanceof Select) {
            Select select = (Select)command;
            if (select.getGroupBy() == null && select.getDerivedColumns().get(0).getExpression() instanceof AggregateFunction) {
                this.isCount = true;
            }
        }
        if (!this.visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }

        this.expectedColumnTypes = command.getColumnTypes();
    }

    @Override
    public void execute() throws TranslatorException {
        final String URI = this.visitor.buildURL("");

        if (isCount) {
            Map<String, List<String>> headers = new TreeMap<String, List<String>>();
            headers.put("Accept", Arrays.asList("text/plain"));  //$NON-NLS-1$ //$NON-NLS-2$

            BinaryWSProcedureExecution execution = invokeHTTP("GET", URI, null, headers); //$NON-NLS-1$
            if (execution.getResponseCode() != HttpStatusCode.OK.getStatusCode()) {
                throw buildError(execution);
            }

            Blob blob = (Blob)execution.getOutputParameterValues().get(0);
            try {
                this.countResponse = Integer.parseInt(ObjectConverterUtil.convertToString(blob.getBinaryStream()));
            } catch (IOException e) {
                throw new TranslatorException(e);
            } catch (SQLException e) {
                throw new TranslatorException(e);
            }
        } else {
            InputStream payload = executeQuery(
                    "GET", URI, null, null, ////$NON-NLS-1$
                    new HttpStatusCode[] {
                            HttpStatusCode.OK,
                            HttpStatusCode.NO_CONTENT,
                            HttpStatusCode.NOT_FOUND
                    });
            this.response = new ODataResponse(payload,
                    ODataType.ENTITY_COLLECTION, this.visitor.getODataQuery().getRootDocument()) {
                @Override
                public InputStream nextBatch(java.net.URI uri) throws TranslatorException {
                    return executeSkipToken(uri, URI.toString(),
                            new HttpStatusCode[] {
                        HttpStatusCode.OK,
                        HttpStatusCode.NO_CONTENT,
                        HttpStatusCode.NOT_FOUND
                    });
                }
            };
        }
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {

        if (isCount && this.countResponse != -1) {
            int count = this.countResponse;
            this.countResponse = -1;
            return Arrays.asList(count);
        }

        if (this.response != null) {
            Map<String, Object> row = this.response.getNext();
            if (row != null) {
                return buildRow(visitor.getODataQuery().getRootDocument().getTable(),
                        visitor.getProjectedColumns(),
                        this.expectedColumnTypes, row);
            }
        }
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }
}
