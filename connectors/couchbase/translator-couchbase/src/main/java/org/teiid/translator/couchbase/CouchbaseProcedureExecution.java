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

import static org.teiid.translator.couchbase.CouchbaseProperties.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.language.Call;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

public class CouchbaseProcedureExecution extends CouchbaseExecution implements ProcedureExecution {

    private final Call call;

    private N1QLVisitor visitor;
    private Iterator<N1qlQueryRow> results;

    protected CouchbaseProcedureExecution(CouchbaseExecutionFactory executionFactory, Call call, ExecutionContext executionContext, RuntimeMetadata metadata, CouchbaseConnection connection) {
        super(executionFactory, executionContext, metadata, connection);
        this.call = call;
    }

    @Override
    public void execute() throws TranslatorException {

        this.visitor = this.executionFactory.getN1QLVisitor();
        this.visitor.append(call);
        String n1ql = this.visitor.toString();
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29002, call, n1ql));
        executionContext.logCommand(n1ql);
        N1qlQueryResult queryResult = connection.execute(n1ql);
        this.results = queryResult.iterator();
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {

        if(this.results != null && this.results.hasNext()) {
            final N1qlQueryRow row = this.results.next();
            String procName = this.call.getProcedureName();
            if(procName.equalsIgnoreCase(GETDOCUMENTS) || procName.equalsIgnoreCase(GETDOCUMENT)) {
                ArrayList<Object> result = new ArrayList<>(1);
                InputStreamFactory isf = new InputStreamFactory() {
                    @Override
                    public InputStream getInputStream() throws IOException {
                        return new ByteArrayInputStream(row.byteValue());
                    }
                };
                Object value = new BlobType(new BlobImpl(isf));
                result.add(value);
                return result;
            }
        }

        return null;
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return Collections.emptyList();// not define out parameter
    }

    @Override
    public void cancel() throws TranslatorException {
        close();
    }

    @Override
    public void close() {
        this.results = null;
    }

}
