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
package org.teiid.translator.odata4;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.codec.Charsets;
import org.apache.olingo.commons.api.ODataError;
import org.apache.olingo.commons.api.data.ComplexValue;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ResWrap;
import org.apache.olingo.commons.api.format.AcceptType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.commons.api.serialization.ODataDeserializerException;
import org.apache.olingo.commons.core.serialization.JsonDeserializer;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.Literal;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.WSConnection;
import org.teiid.translator.odata4.ODataMetadataProcessor.ODataType;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

public class BaseQueryExecution {
	protected WSConnection connection;
	protected ODataExecutionFactory translator;
	protected RuntimeMetadata metadata;
	protected ExecutionContext executionContext;

    public BaseQueryExecution(ODataExecutionFactory translator,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            WSConnection connection) {
		this.metadata = metadata;
		this.executionContext = executionContext;
		this.translator = translator;
		this.connection = connection;
	}

    protected InputStream executeQuery(String method,
            String uri, String payload, String eTag, HttpStatusCode... expectedStatus)
            throws TranslatorException {
        
        Map<String, List<String>> headers = getDefaultHeaders();
        if (eTag != null) {
            headers.put("If-Match", Arrays.asList(eTag)); //$NON-NLS-1$
        }

        if (payload != null) {
            headers.put("Content-Type", Arrays.asList(ContentType.APPLICATION_JSON.toContentTypeString())); //$NON-NLS-1$ //$NON-NLS-2$
        }

        BinaryWSProcedureExecution execution;
        try {
            execution = invokeHTTP(method, uri, payload, headers);
            for (HttpStatusCode status:expectedStatus) {
                if (status.getStatusCode() == execution.getResponseCode()) {
                    if (execution.getResponseCode() != HttpStatusCode.NO_CONTENT.getStatusCode() 
                            && execution.getResponseCode() != HttpStatusCode.NOT_FOUND.getStatusCode()) {
                        Blob blob = (Blob)execution.getOutputParameterValues().get(0);
                        return blob.getBinaryStream();
                    }
                    // this is success with no-data
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new TranslatorException(e);
        }
        // throw an error
        throw buildError(execution);
        
    }
    
    protected ODataEntitiesResponse executeWithReturnEntity(String method,
            String uri, String payload, String eTag, HttpStatusCode... expectedStatus)
            throws TranslatorException {
		
        try {
            InputStream result = executeQuery(method, uri, payload, eTag, expectedStatus);
            if (result == null) {
                // this is success with no-data
                return new ODataEntitiesResponse();
            }
            JsonDeserializer parser = new JsonDeserializer(false);
            ResWrap<EntityCollection> collection = parser.toEntitySet(result);
            return new ODataEntitiesResponse(uri, collection.getPayload());            
        } catch (TranslatorException e) {
            return new ODataEntitiesResponse(e); 
        } catch(ODataDeserializerException e) {
            throw new TranslatorException(e);
        }
	}
	
	String getHeader(BinaryWSProcedureExecution execution, String header) {
		Object value = execution.getResponseHeader(header);
		if (value instanceof List) {
			return (String)((List<?>)value).get(0);
		}
		return (String)value;
	}	

	protected TranslatorException buildError(BinaryWSProcedureExecution execution) {
		// do some error handling
		try {
			Blob blob = (Blob)execution.getOutputParameterValues().get(0);
			JsonDeserializer parser = new JsonDeserializer(false);
			ODataError error = parser.toError(blob.getBinaryStream());			
            return new TranslatorException(ODataPlugin.Util.gs(
                    ODataPlugin.Event.TEIID17013, execution.getResponseCode(),
                    error.getCode(), error.getMessage(), error.getInnerError()));
		}
		catch (Throwable t) {
			return new TranslatorException(t);
		}
	}

    protected BinaryWSProcedureExecution invokeHTTP(String method,
            String uri, String payload, Map<String, List<String>> headers)
            throws TranslatorException {

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_ODATA, MessageLevel.DETAIL)) {
			try {
                LogManager.logDetail(LogConstants.CTX_ODATA,
                        "Source-URL=", URLDecoder.decode(uri, "UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
			} catch (UnsupportedEncodingException e) {
			}
		}

		List<Argument> parameters = new ArrayList<Argument>();
		parameters.add(new Argument(Direction.IN, 
		        new Literal(method, TypeFacility.RUNTIME_TYPES.STRING), null));
		parameters.add(new Argument(Direction.IN, 
		        new Literal(payload, TypeFacility.RUNTIME_TYPES.STRING), null));
		parameters.add(new Argument(Direction.IN, 
		        new Literal(uri, TypeFacility.RUNTIME_TYPES.STRING), null));
		parameters.add(new Argument(Direction.IN, 
		        new Literal(true, TypeFacility.RUNTIME_TYPES.BOOLEAN), null));
		//the engine currently always associates out params at resolve time even if the 
		// values are not directly read by the call
		parameters.add(new Argument(Direction.OUT, TypeFacility.RUNTIME_TYPES.STRING, null));
		
        Call call = this.translator.getLanguageFactory().createCall(
                ODataExecutionFactory.INVOKE_HTTP, parameters, null);

        BinaryWSProcedureExecution execution = new BinaryWSProcedureExecution(
                call, this.metadata, this.executionContext, null,
                this.connection);
		execution.setUseResponseContext(true);
		execution.setCustomHeaders(headers);
		execution.execute();
		return execution;
	}

	protected Map<String, List<String>> getDefaultHeaders() {
		Map<String, List<String>> headers = new HashMap<String, List<String>>();
		headers.put("Accept", Arrays.asList(AcceptType.fromContentType(ContentType.APPLICATION_JSON).toString())); //$NON-NLS-1$
		headers.put("Content-Type", Arrays.asList(ContentType.APPLICATION_JSON.toContentTypeString())); //$NON-NLS-1$ //$NON-NLS-2$
		return headers;
	}

	class ODataEntitiesResponse {
		private EntityCollection feed;
		private String uri;
		private Iterator<Entity> rowIter;
		private TranslatorException exception;
		private HttpStatusCode[] acceptedStatus;
		private List<List<?>> currentRow;

        public ODataEntitiesResponse() {
        }
        
        public ODataEntitiesResponse(String uri, EntityCollection feed,
                HttpStatusCode... accptedStatus) {
			this.uri = uri;
			this.feed = feed;
			this.rowIter = this.feed.getEntities().iterator();
			this.acceptedStatus = accptedStatus;
		}

		public ODataEntitiesResponse(TranslatorException ex) {
			this.exception = ex;
		}

		public boolean hasRow() {
			return (this.rowIter != null && this.rowIter.hasNext());
		}

		public boolean hasError() {
			return this.exception != null;
		}

		public TranslatorException getError() {
			return this.exception;
		}

	    private boolean isComplexType(Table table) {
	        ODataType type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
	        return type == ODataType.COMPLEX || type == ODataType.COMPLEX_COLLECTION;
	    }
	    
        private boolean isCollection(Table table) {
            ODataType type = ODataType.valueOf(table.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
            return type == ODataType.ENTITYSET
                    || type == ODataType.COMPLEX_COLLECTION
                    || type == ODataType.NAVIGATION_COLLECTION;
        }	    
	    
	    public String getName(Table table) {
	        if (table.getNameInSource() != null) {
	            return table.getNameInSource();
	        }
	        return table.getName();
	    }
	    
        public Property getProperty(final String name, final List<Property> properties) {
            Property result = null;

            for (Property property : properties) {
                if (name.equals(property.getName())) {
                    result = property;
                    break;
                }
            }
            return result;
        }	    
	    
        private List<List<?>> walkEntity(Entity entity, Table entitySetTable, Column[] columns,
                Class<?>[] expectedType) throws TranslatorException{
            
            // first read any complex values
            List<LinkedHashMap<String, Object>> complexRows = null;
            for (int i = 0; i < columns.length; i++) {
                Column column = columns[i];
                Table columnParent = (Table)column.getParent();
                if (!columnParent.equals(entitySetTable)) {
                    String complexColumn = getName(columnParent);
                    if (complexColumn.indexOf('/') != -1) {
                        complexColumn = complexColumn.substring(0, complexColumn.indexOf('/'));
                    }
                    Property property = entity.getProperty(complexColumn);
                    if(isComplexType(columnParent)) {
                        complexRows = unwindComplex(property, complexRows);
                    }
                }
            }
            
            List<List<?>> results = new ArrayList<List<?>>();
            if (complexRows != null) {
                for (LinkedHashMap<String, Object> row:complexRows) {
                    results.add(buildRow(entity, entitySetTable, columns, expectedType, row));
                }
            } else {
                results.add(buildRow(entity, entitySetTable, columns, expectedType, null));
            }
            return results;
        }
        
        private List<?> buildRow(Entity entity, Table entitySetTable, Column[] columns,
                Class<?>[] expectedType, Map<String, Object> other) throws TranslatorException {
            List<Object> results = new ArrayList<Object>();
            for (int i = 0; i < columns.length; i++) {
                Column column = columns[i];
                Table columnParent = (Table)column.getParent();
                List<Property> properties = entity.getProperties();
                if (columnParent.equals(entitySetTable)) {
                    Object value = ODataTypeManager.convertTeiidInput(
                            getProperty(columns[i].getName(), properties).getValue(), expectedType[i]);
                    results.add(value);
                } else {
                    Object value = null;
                    if (other != null) {
                        value = ODataTypeManager.convertTeiidInput(
                                other.get(columns[i].getName()), expectedType[i]);
                    }
                    results.add(value);
                }
            }
            return results;
        }

        private List<LinkedHashMap<String, Object>> unwindComplexValue(
                final ComplexValue complexValue,
                final List<LinkedHashMap<String, Object>> previousRows) {
            
            List<LinkedHashMap<String, Object>> rows = null;
            if (previousRows != null) {
                rows = new ArrayList<LinkedHashMap<String, Object>>();
                rows.addAll(previousRows);
            }
            
            // read all non-complex properties
            LinkedHashMap<String, Object> row = new LinkedHashMap<String, Object>();
            List<Property> complexProperties = complexValue.getValue();
            for (Property p:complexProperties) {
                if (p.isPrimitive()) {
                    row.put(p.getName(), p.asPrimitive());
                }
            }

            // now read all complex
            Stack<List<LinkedHashMap<String, Object>>> stack = new Stack<List<LinkedHashMap<String,Object>>>();
            for (Property p:complexProperties) {
                if (p.isComplex()) {
                    stack.push(unwindComplex(p, previousRows));
                }
            }

            while (!stack.isEmpty()) {
                if (previousRows == null) {
                    rows = stack.pop();
                } else {
                    ArrayList<LinkedHashMap<String, Object>> crossjoin = new ArrayList<LinkedHashMap<String,Object>>();
                    List<LinkedHashMap<String, Object>> inner = stack.pop();
                    for (LinkedHashMap<String, Object> r1:rows) {                            
                        for (LinkedHashMap<String, Object> r2:inner) {
                            LinkedHashMap<String, Object> copy = new LinkedHashMap<String, Object>();
                            copy.putAll(r2);
                            copy.putAll(r1);
                            crossjoin.add(copy);
                        }
                    }
                    rows = crossjoin;
                }
            }
            
            if (rows == null) {
                rows = new ArrayList<LinkedHashMap<String,Object>>();
                rows.add(row);
            } else {
                for (LinkedHashMap<String,Object> r:rows) {
                    r.putAll(row);
                }
            }
            return rows;
        }
        
        private List<LinkedHashMap<String, Object>> unwindComplex(
                Property property, List<LinkedHashMap<String, Object>> rows) {
            if (property.isCollection()) {
                List<ComplexValue> complexRows = (List<ComplexValue>)property.asCollection();                 
                for (ComplexValue complexRow : complexRows) {
                    rows = unwindComplexValue(complexRow, rows);
                }
            } else {
                rows = unwindComplexValue(property.asComplex(), rows);
            }
            return rows;
        }

        public List<?> getNextRow(Table entitySetTable, Column[] columns,
                Class<?>[] expectedType) throws TranslatorException {

            // we already walking a complex document, keep reading it
            if (this.currentRow != null && !this.currentRow.isEmpty()) {
                return this.currentRow.remove(0);
            }
            
            // move on to next document in row.
            if (this.rowIter != null && this.rowIter.hasNext()) {
				Entity entity = this.rowIter.next();
				this.currentRow = walkEntity(entity, entitySetTable, columns, expectedType);
				fetchNextBatch(!this.rowIter.hasNext());
	            if (this.currentRow != null && !this.currentRow.isEmpty()) {
	                return this.currentRow.remove(0);
	            }
			}
			return null;
		}

		// TODO:there is possibility here to async execute this feed
		private void fetchNextBatch(boolean fetch) throws TranslatorException {
			if (!fetch) {
				return;
			}

			String next = this.feed.getNext().toString();
			if (next == null) {
				this.feed = null;
				this.rowIter = null;
				return;
			}

			int idx = next.indexOf("$skiptoken="); //$NON-NLS-1$
			if (idx != -1) {
				String skip = null;
				try {
					skip = next.substring(idx + 11);
					skip = URLDecoder.decode(skip, Charsets.UTF_8.name());
				} catch (UnsupportedEncodingException e) {
					throw new TranslatorException(e);
				}

				String nextUri = this.uri;
				if (this.uri.indexOf('?') == -1) {
					nextUri = this.uri + "?$skiptoken="+skip; //$NON-NLS-1$
				}
				else {
					nextUri = this.uri + "&$skiptoken="+skip; //$NON-NLS-1$
				}
				getNext(nextUri, this.acceptedStatus);
			} else if (next.toLowerCase().startsWith("http")) { //$NON-NLS-1$
			    getNext(next, this.acceptedStatus);
			} else {
				throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17001, next));
			}
		}

		private void getNext(String uri, HttpStatusCode[] accptedCodes) throws TranslatorException {
            try {
                BinaryWSProcedureExecution execution = invokeHTTP("GET", uri, null, getDefaultHeaders()); //$NON-NLS-1$
                for (HttpStatusCode expected : accptedCodes) {
                    if (execution.getResponseCode() != expected.getStatusCode()) {
                        throw buildError(execution);
                    }
                }
                
                Blob blob = (Blob)execution.getOutputParameterValues().get(0);
                JsonDeserializer parser = new JsonDeserializer(false);
                ResWrap<EntityCollection> result = parser.toEntitySet(blob.getBinaryStream());
                
                this.feed = result.getPayload();                
                this.rowIter = this.feed.getEntities().iterator();
            } catch (ODataDeserializerException e) {
                throw new TranslatorException(e);
            } catch (SQLException e) {
                throw new TranslatorException(e);
            }
		}
	}
}
