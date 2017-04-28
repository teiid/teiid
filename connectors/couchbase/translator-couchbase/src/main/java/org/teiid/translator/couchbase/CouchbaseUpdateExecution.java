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
                    count += results.allRows().size();
                }
            }
            
        } else {
            results = executeDirect(visitor.toString());
            if(results != null) {
                count = results.allRows().size();
            }
        } 
        
        if(count > 0) {
            this.returns = new int[1];
            this.returns[0] = count;
        }
    }
    
    private N1qlQueryResult executeDirect(String n1ql) {
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
