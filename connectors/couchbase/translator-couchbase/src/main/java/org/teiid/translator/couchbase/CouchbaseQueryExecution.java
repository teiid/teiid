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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

public class CouchbaseQueryExecution extends CouchbaseExecution implements ResultSetExecution {
    
	private Select command;
	private CouchbaseExecutionFactory executionFactory;
	private Class<?>[] expectedTypes;
	
	private N1QLVisitor visitor;
	private Iterator<N1qlQueryRow> results;
	private Iterator <Object> array;

	public CouchbaseQueryExecution(
			CouchbaseExecutionFactory executionFactory,
			QueryExpression command, ExecutionContext executionContext,
			RuntimeMetadata metadata, CouchbaseConnection connection) {
		super(executionContext, metadata, connection);
		this.command = (Select)command;
		this.executionFactory = executionFactory;
		this.expectedTypes = command.getColumnTypes();
	}

	@Override
	public void execute() throws TranslatorException {
		this.visitor = this.executionFactory.getN1QLVisitor(metadata);
		this.visitor.append(this.command);
		LogManager.logInfo(LogConstants.CTX_CONNECTOR, this.command);
		String sql = this.visitor.toString();
		N1qlQueryResult queryResult = connection.executeQuery(sql);
		this.results = queryResult.iterator();
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
	    if (this.results != null && this.results.hasNext() && this.array == null) {
	        N1qlQueryRow queryRow = this.results.next();
	        if(queryRow != null) {
	            List<Object> row = new ArrayList<>(expectedTypes.length);
	            JsonObject json = queryRow.value();
	            for(int i = 0 ; i < expectedTypes.length ; i ++){
	                Object value = json.get(this.visitor.getSelectColumns().get(i));
	                if(value instanceof JsonArray) {
	                    array = ((JsonArray)value).iterator();
	                    return nextArray();
	                }
	                row.add(this.executionFactory.retrieveValue(this.visitor.getSelectColumns().get(i), expectedTypes[i], value));
	            }
	            return row;
	        }
	    } else if(this.array != null && this.array.hasNext()) {
	        return nextArray();
	    }
		return null;
	}

	private List<?> nextArray() {
	    if(this.array != null && this.array.hasNext()){
	        List<Object> row = new ArrayList<>(1);
            row.add(this.array.next());
            if(!this.array.hasNext()) {
                this.array = null;
            }
            return row;
	    }
        return null;
    }

    @Override
	public void close() {
	    this.results = null;
	}

	@Override
	public void cancel() throws TranslatorException {
		close();
	}
}
