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

import static org.teiid.translator.couchbase.CouchbaseProperties.PLACEHOLDER;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

public class CouchbaseQueryExecution extends CouchbaseExecution implements ResultSetExecution {
    
	private QueryExpression command;
	private Class<?>[] expectedTypes;
	
	private N1QLVisitor visitor;
	private Iterator<N1qlQueryRow> results;
	
	public CouchbaseQueryExecution(
			CouchbaseExecutionFactory executionFactory,
			QueryExpression command, ExecutionContext executionContext,
			RuntimeMetadata metadata, CouchbaseConnection connection) {
		super(executionFactory, executionContext, metadata, connection);
		this.command = command;
		this.expectedTypes = command.getColumnTypes();
	}

	@Override
	public void execute() throws TranslatorException {
		this.visitor = this.executionFactory.getN1QLVisitor();
		this.visitor.append(this.command);
		String n1ql = this.visitor.toString();
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29001, n1ql));
		executionContext.logCommand(n1ql);
		N1qlQueryResult queryResult = connection.executeQuery(n1ql);
		this.results = queryResult.iterator();
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
	    
	    if (this.results != null && this.results.hasNext()) {
	        N1qlQueryRow queryRow = this.results.next();
	        if(queryRow != null) {
	            List<Object> row = new ArrayList<>(expectedTypes.length);
	            JsonObject json = queryRow.value();
	            
	            int placeholderIndex = 1;
	            for(int i = 0 ; i < expectedTypes.length ; i ++){
	                String columnName = null;
	                Object value = null;
	                int cursor = i + 1;
	                
	                // column without reference, like 'select col, count(*) from table'
	                if(cursor <= this.visitor.getSelectColumns().size()){
	                    columnName = this.visitor.getSelectColumns().get(i);
	                    value = json.get(columnName); 
	                }
	                
	                // column with alias, like 'select col AS c_1 from table' 
	                if(value == null && (cursor <= this.visitor.getSelectColumnReferences().size()) && this.visitor.getSelectColumnReferences().get(i) != null) {
	                    columnName = this.visitor.getSelectColumnReferences().get(i);
	                    value = json.get(columnName);
	                }
	                
	                // column without reference and alias
	                String placeholder = buildPlaceholder(placeholderIndex);
	                if(value == null && json.getNames().contains(placeholder)) {
	                    columnName = placeholder;
	                    value = json.get(columnName);
	                    placeholderIndex++ ;
	                }

	                row.add(this.executionFactory.retrieveValue(expectedTypes[i], value));
	            }
	            return row;
	        }
	    } 
		return null;
	}

    private String buildPlaceholder(int i) {
        return PLACEHOLDER + i; 
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
