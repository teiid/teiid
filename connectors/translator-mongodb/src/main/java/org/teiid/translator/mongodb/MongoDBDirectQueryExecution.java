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
package org.teiid.translator.mongodb;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
/**
 * This enables the Direct Query execution of the MongoDB queries. For that to happen the procedure 
 * invocation needs to be like
 * {code}
 * 		native('CollectionName;{$match : { \"CompanyName\" : \"A\"}};{$project : {\"CompanyName\", "userName}}', [param1, param2])
 * {code} 
 * the first parameter must be collection name, then the arguments must match the aggregation pipeline structure delimited by
 * semi-colon (;) between each pipeline statement
 * 
 *      select x.* from TABLE(call native('city;{$match:{"city":"FREEDOM"}}')) t, 
		xmltable('/city' PASSING JSONTOXML('city', cast(array_get(t.tuple, 1) as BLOB)) COLUMNS city string, state string) x
 *     
 *     select JSONTOXML('city', cast(array_get(t.tuple, 1) as BLOB)) as col from TABLE(call native('city;{$match:{"city":"FREEDOM"}}')) t;
 */
public class MongoDBDirectQueryExecution extends MongoDBBaseExecution implements ProcedureExecution {
	private String query;
	private List<Argument> arguments;
	protected boolean returnsArray;
	private Cursor results;
	private MongoDBExecutionFactory executionFactory;
	
	public MongoDBDirectQueryExecution(List<Argument> arguments, @SuppressWarnings("unused") Command cmd,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			MongoDBConnection connection, String nativeQuery, boolean returnsArray, MongoDBExecutionFactory ef) {
		super(executionContext, metadata, connection);
		this.arguments = arguments;
		this.returnsArray = returnsArray;
		this.query = nativeQuery;
		this.executionFactory = ef;
	}

	@Override
	public void execute() throws TranslatorException {
		StringBuilder buffer = new StringBuilder();		
		SQLStringVisitor.parseNativeQueryParts(query, arguments, buffer, new SQLStringVisitor.Substitutor() {
			@Override
			public void substitute(Argument arg, StringBuilder builder, int index) {
				Literal argumentValue = arg.getArgumentValue();
				builder.append(argumentValue.getValue());
			}
		});
		
		StringTokenizer st = new StringTokenizer(buffer.toString(), ";"); //$NON-NLS-1$
		String collectionName = st.nextToken();
		boolean shellOperation = collectionName.equalsIgnoreCase("$ShellCmd"); //$NON-NLS-1$
		String shellOperationName = null;
		if (shellOperation) {
		    collectionName = st.nextToken();
		    shellOperationName = st.nextToken();
		}
	
		DBCollection collection = this.mongoDB.getCollection(collectionName);
		if (collection == null) {
			throw new TranslatorException(MongoDBPlugin.Event.TEIID18020, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18020, collectionName));
		}
		
		if (shellOperation) {
		    
            ArrayList<Object> operations = new ArrayList<Object>();
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                if (token.startsWith("{")) { //$NON-NLS-1$
                    operations.add(JSON.parse(token));
                }
                else {
                    operations.add(token);
                }
            }
            
		    try {
		        ReflectionHelper helper = new ReflectionHelper(DBCollection.class);
                Method method = helper.findBestMethodOnTarget(shellOperationName, operations.toArray(new Object[operations.size()]));
                Object result = method.invoke(collection, operations.toArray(new Object[operations.size()]));
                if (result instanceof Cursor) {
                    this.results = (Cursor)result;
                }
                else if (result instanceof WriteResult) {
                    WriteResult wr = (WriteResult)result;
                    if (wr.getError() != null) {
                        throw new TranslatorException(wr.getError());
                    }
                }
            } catch (NoSuchMethodException e) {
                throw new TranslatorException(MongoDBPlugin.Event.TEIID18034, e, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18034, buffer.toString()));
            } catch (SecurityException e) {
                throw new TranslatorException(MongoDBPlugin.Event.TEIID18034, e, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18034, buffer.toString()));
            } catch (IllegalAccessException e) {
                throw new TranslatorException(MongoDBPlugin.Event.TEIID18034, e, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18034, buffer.toString()));
            } catch (IllegalArgumentException e) {
                throw new TranslatorException(MongoDBPlugin.Event.TEIID18034, e, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18034, buffer.toString()));
            } catch (InvocationTargetException e) {
                throw new TranslatorException(MongoDBPlugin.Event.TEIID18034, e.getTargetException(), MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18034, buffer.toString()));
            }
		}
		else {
	        ArrayList<DBObject> operations = new ArrayList<DBObject>();
	        while (st.hasMoreTokens()) {
	            String token = st.nextToken();
	            operations.add((DBObject)JSON.parse(token));
	        }
		    
    		if (operations.isEmpty()) {
    			throw new TranslatorException(MongoDBPlugin.Event.TEIID18021, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18021, collectionName));
    		}
    		
            this.results = collection.aggregate(operations,
                    this.executionFactory.getOptions(this.executionContext.getBatchSize()));
		}
	}

	@Override
	public List<?> getOutputParameterValues() throws TranslatorException {
		return null;
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		final DBObject value = nextRow();
		if (value == null) {
			return null;
		}
		
		BlobType result = new BlobType(new BlobImpl(new InputStreamFactory() {
        	@Override
        	public InputStream getInputStream() throws IOException {
        		return new ByteArrayInputStream(JSON.serialize(value).getBytes(Streamable.CHARSET));
        	}
        }));
		
		if (returnsArray) {
			List<Object[]> row = new ArrayList<Object[]>(1);
			row.add(new Object[] {result});
			return row;
		}
		return Arrays.asList(result);
	}	
	
	@SuppressWarnings("unused")
	public DBObject nextRow() throws TranslatorException, DataNotAvailableException {
		if (this.results != null && this.results.hasNext()) {
			DBObject result = this.results.next();
			return result;
		}
		return null;
	}	
	
	@Override
	public void close() {
	    if (this.results != null) {
	        this.results.close();
	        this.results = null;
	    }
	}

	@Override
	public void cancel() throws TranslatorException {
		close();
	}	
}
