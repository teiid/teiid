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

import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.language.Command;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

import com.couchbase.client.java.query.N1qlQueryResult;

public class CouchbaseUpdateExecution extends CouchbaseExecution implements UpdateExecution {

    private Command command;
    private N1QLUpdateVisitor visitor;
    private int[] returns = new int[] {0};

    protected CouchbaseUpdateExecution(Command command, CouchbaseExecutionFactory ef, ExecutionContext context, RuntimeMetadata metadata, CouchbaseConnection conn) {
        super(ef, context, metadata, conn);
        this.command = command;
    }

    @Override
    public void execute() throws TranslatorException {
        this.visitor = this.executionFactory.getN1QLUpdateVisitor();
        this.visitor.append(this.command);

        N1qlQueryResult results;
        int count = 0;
        if(visitor.getBulkCommands() != null) {
            for(String n1ql : visitor.getBulkCommands()) {
                results = executeDirect(n1ql);
                if(results != null) {
                    count += results.info().mutationCount();
                }
            }

        } else {
            results = executeDirect(visitor.toString());
            if(results != null) {
                count = results.info().mutationCount();
            }
        }

        if(count > 0) {
            this.returns = new int[1];
            this.returns[0] = count;
        }
    }

    private N1qlQueryResult executeDirect(String n1ql) throws TranslatorException {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29004, n1ql));
        executionContext.logCommand(n1ql);
        return this.connection.execute(n1ql);
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        return this.returns;
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }

}
