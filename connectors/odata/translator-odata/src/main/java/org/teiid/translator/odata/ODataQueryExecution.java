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
package org.teiid.translator.odata;

import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.core.Response.Status;

import org.odata4j.edm.EdmDataServices;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Schema;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;

public class ODataQueryExecution extends BaseQueryExecution implements ResultSetExecution {

    private ODataSQLVisitor visitor;
    private int countResponse = -1;
    private Class<?>[] expectedColumnTypes;
    private ODataEntitiesResponse response;

    public ODataQueryExecution(ODataExecutionFactory translator,
            QueryExpression command, ExecutionContext executionContext,
            RuntimeMetadata metadata, WSConnection connection) throws TranslatorException {
        super(translator, executionContext, metadata, connection);

        this.visitor = new ODataSQLVisitor(this.translator, metadata);
        this.visitor.visitNode(command);
        if (!this.visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }

        this.expectedColumnTypes = command.getColumnTypes();
    }

    @Override
    public void execute() throws TranslatorException {
        String URI = this.visitor.buildURL();

        if (this.visitor.isCount()) {
            Map<String, List<String>> headers = new TreeMap<String, List<String>>();
            headers.put("Accept", Arrays.asList("text/xml", "text/plain"));  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

            BinaryWSProcedureExecution execution = executeDirect("GET", URI, null, headers); //$NON-NLS-1$
            if (execution.getResponseCode() != Status.OK.getStatusCode()) {
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
        }
        else {
            Schema schema = visitor.getEnityTable().getParent();
            EdmDataServices edm = new TeiidEdmMetadata(schema.getName(), ODataEntitySchemaBuilder.buildMetadata( schema));
            this.response = executeWithReturnEntity("GET", URI, null, visitor.getEnityTable().getName(), edm, null, Status.OK, Status.NO_CONTENT, Status.NOT_FOUND); //$NON-NLS-1$
            if (this.response != null && this.response.hasError()) {
                throw this.response.getError();
            }
        }
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (visitor.isCount() && this.countResponse != -1) {
            int count = this.countResponse;
            this.countResponse = -1;
            return Arrays.asList(count);
        }

        // Feed based response
        if (this.response != null && !this.response.hasError()) {
            return this.response.getNextRow(visitor.getSelect(), this.expectedColumnTypes);
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
