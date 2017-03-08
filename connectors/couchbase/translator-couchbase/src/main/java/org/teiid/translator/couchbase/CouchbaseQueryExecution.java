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

import static org.teiid.translator.couchbase.CouchbaseMetadataProcessor.buildPlaceholder;

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

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

public class CouchbaseQueryExecution extends CouchbaseExecution implements ResultSetExecution {
    
	private QueryExpression command;
	private Class<?>[] expectedTypes;
	
	private N1QLVisitor visitor;
	private Iterator<N1qlQueryRow> results;
	private Iterator <Object> array;
	private List<Object> arrayRow;
	private List<String> arrayColumnNames;

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
		String sql = this.visitor.toString();
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29001, sql));
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
	                if(value == null && json.getNames().contains(buildPlaceholder(1))) {
	                    columnName = buildPlaceholder(1);
	                    value = json.get(columnName);
	                }
	                
	                if(value instanceof JsonArray) {
	                    array = ((JsonArray)value).iterator();
	                    this.arrayRow = row;
	                    this.arrayColumnNames = this.visitor.getSelectColumns().subList(i +1, this.visitor.getSelectColumns().size());
	                    return nextArray();
	                }
	                row.add(this.executionFactory.retrieveValue(columnName, expectedTypes[i], value));
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
	        
	        List<Object> row = new ArrayList<>();
	        row.addAll(this.arrayRow);
	        Object obj = this.array.next();
	        JsonObject json = null;
	        boolean isAddObject = true;
	        
	        if(obj instanceof JsonObject) {
	            json = (JsonObject) obj;
	        }
	        
	        for(int i = 1; i < expectedTypes.length ; i ++) {
	            String columnName = arrayColumnNames.get(i - 1);
	            if(json != null) {
                    row.add(this.executionFactory.retrieveValue(columnName, expectedTypes[i], json.get(columnName)));
                } else if(isAddObject){
                    row.add(this.executionFactory.retrieveValue(columnName, expectedTypes[i], obj));
                    isAddObject = false;
                } else {
                    row.add(this.executionFactory.retrieveValue(columnName, expectedTypes[i], null));
                }
	        }

            if(!this.array.hasNext()) {
                this.array = null;
                this.arrayRow = null;
                this.arrayColumnNames = null;
            }
            return row;
	    }
        return null;
    }

    @Override
	public void close() {
	    this.results = null;
	    this.array = null;
        this.arrayRow = null;
        this.arrayColumnNames = null;
	}

	@Override
	public void cancel() throws TranslatorException {
		close();
	}
}
