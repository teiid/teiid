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
package org.teiid.translator.couchbase;

import static org.teiid.translator.couchbase.CouchbaseProperties.GETDOCUMENT;
import static org.teiid.translator.couchbase.CouchbaseProperties.GETDOCUMENTS;


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
        String sql = this.visitor.toString();
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29002, call, sql));
        N1qlQueryResult queryResult = connection.executeQuery(sql);
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
