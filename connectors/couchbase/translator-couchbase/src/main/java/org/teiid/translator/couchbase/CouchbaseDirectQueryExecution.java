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
package org.teiid.translator.couchbase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

import com.couchbase.client.java.query.N1qlQueryRow;

public class CouchbaseDirectQueryExecution extends CouchbaseExecution implements ProcedureExecution {

    private List<Argument> arguments;

    private Iterator<N1qlQueryRow> results;

    public CouchbaseDirectQueryExecution(List<Argument> arguments, Command command, CouchbaseExecutionFactory executionFactory, ExecutionContext executionContext, RuntimeMetadata metadata, CouchbaseConnection connection) {
        super(executionFactory, executionContext, metadata, connection);
        this.arguments = arguments;
    }

    @Override
    public void execute() throws TranslatorException {
        String n1ql = (String)this.arguments.get(0).getArgumentValue().getValue();
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29001, n1ql));
        executionContext.logCommand(n1ql);
        this.results = connection.execute(n1ql).iterator();
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        ArrayList<Object[]> returns = new ArrayList<>(1);
        ArrayList<Object> result = new ArrayList<>(1);
        if(this.results != null && this.results.hasNext()) {
            final N1qlQueryRow row = this.results.next();
            InputStreamFactory isf = new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return new ByteArrayInputStream(row.byteValue());
                }
            };
            result.add(new BlobType(new BlobImpl(isf)));
            returns.add(result.toArray());
            return returns;
        } else {
            return null;
        }

    }

    @Override
    public void close() {
        results = null;
    }

    @Override
    public void cancel() throws TranslatorException {
        close();
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return null;
    }

}
