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
package org.teiid.translator.odata;

import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import org.odata4j.core.ODataConstants;
import org.odata4j.core.ODataConstants.Charsets;
import org.odata4j.core.ODataVersion;
import org.odata4j.core.OEntity;
import org.odata4j.core.OError;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.format.Entry;
import org.odata4j.format.Feed;
import org.odata4j.format.FormatParser;
import org.odata4j.format.FormatParserFactory;
import org.odata4j.format.FormatType;
import org.odata4j.format.Settings;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.Literal;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.WSConnection;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

public class BaseQueryExecution {
	protected WSConnection connection;
	protected ODataExecutionFactory translator;
	protected RuntimeMetadata metadata;
	protected ExecutionContext executionContext;
	protected EdmDataServices edsMetadata;
	
	public BaseQueryExecution(ODataExecutionFactory translator, ExecutionContext executionContext,
			RuntimeMetadata metadata, WSConnection connection, EdmDataServices edsMetadata) {
		this.metadata = metadata;
		this.executionContext = executionContext;
		this.translator = translator;
		this.connection = connection;
		this.edsMetadata = edsMetadata;		
	}

	protected Feed parse(Blob blob, ODataVersion version, String entityTable) throws TranslatorException {
		try {
			// if parser is written to return raw objects; then we can avoid some un-necessary object creation
			// due to time, I am not pursuing that now.
			FormatParser<Feed> parser = FormatParserFactory.getParser(
					Feed.class, FormatType.ATOM, new Settings(version, this.edsMetadata, entityTable, null));
			return parser.parse(new InputStreamReader(blob.getBinaryStream()));
		} catch (SQLException e) {
			throw new TranslatorException(ODataPlugin.Event.TEIID17010, e, e.getMessage());
		}			
	}
	
	protected static ODataVersion getDataServiceVersion(String headerValue) {
		ODataVersion version = ODataConstants.DATA_SERVICE_VERSION;
		if (headerValue != null) {
			String[] str = headerValue.split(";"); //$NON-NLS-1$
			version = ODataVersion.parse(str[0]);
		}
		return version;
	}	
	
	protected ODataEntitiesResponse executeWithReturnEntity(String method, String uri, String payload, String entityTable, String eTag, Status... expectedStatus) throws TranslatorException {
		Map<String, List<String>> headers = getDefaultHeaders();
		if (eTag != null) {
			headers.put("If-Match", Arrays.asList(eTag)); //$NON-NLS-1$
		}
		
		if (payload != null) {
			headers.put("Content-Type", Arrays.asList("application/atom+xml")); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		BinaryWSProcedureExecution execution = executeDirect(method, uri, payload, headers);
		for (Status status:expectedStatus) {
			if (status.getStatusCode() == execution.getResponseCode()) {
				if (execution.getResponseCode() != Status.NO_CONTENT.getStatusCode()) {
					Blob blob = (Blob)execution.getOutputParameterValues().get(0);
					ODataVersion version = getDataServiceVersion((String)execution.getResponseHeader(ODataConstants.Headers.DATA_SERVICE_VERSION));				
					Feed feed = parse(blob, version, entityTable);
					return new ODataEntitiesResponse(uri, feed, entityTable);		
				}
				// this is success with no-data
				return new ODataEntitiesResponse();
			}
		}
		// throw an error
		return new ODataEntitiesResponse(buildError(execution));
	}

	protected TranslatorException buildError(BinaryWSProcedureExecution execution) {
		// do some error handling
		try {
			Blob blob = (Blob)execution.getOutputParameterValues().get(0);
			FormatParser<OError> parser = FormatParserFactory.getParser(OError.class, FormatType.ATOM, null);
			OError error = parser.parse(new InputStreamReader(blob.getBinaryStream()));
			return new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17013, error.getCode(), error.getMessage()));
		}
		catch (Throwable t) {
			return new TranslatorException(t);
		}
	}
	
	protected BinaryWSProcedureExecution executeDirect(String method, String uri, String payload, Map<String, List<String>> headers) throws TranslatorException {
		try {
			LogManager.logDetail(LogConstants.CTX_ODATA, "Source-URL=", URLDecoder.decode(uri, "UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
		} catch (UnsupportedEncodingException e) {
		}

		List<Argument> parameters = new ArrayList<Argument>();
		parameters.add(new Argument(Direction.IN, new Literal(method, TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null));
		parameters.add(new Argument(Direction.IN, new Literal(payload, TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null));
		parameters.add(new Argument(Direction.IN, new Literal(uri, TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null));
		parameters.add(new Argument(Direction.IN, new Literal(true, TypeFacility.RUNTIME_TYPES.BOOLEAN), TypeFacility.RUNTIME_TYPES.BOOLEAN, null));

		Call call = this.translator.getLanguageFactory().createCall(ODataExecutionFactory.INVOKE_HTTP, parameters, null);
		
		BinaryWSProcedureExecution execution = new BinaryWSProcedureExecution(call, this.metadata, this.executionContext, null, this.connection);
		execution.setUseResponseContext(true);
		execution.setAlwaysAllowPayloads(true);
		
		for (String header:headers.keySet()) {
			execution.addHeader(header, headers.get(header));
		}
		execution.execute();		
		return execution;
	}
	
	protected Map<String, List<String>> getDefaultHeaders() {
		Map<String, List<String>> headers = new HashMap<String, List<String>>();
		headers.put("Accept", Arrays.asList(FormatType.ATOM.getAcceptableMediaTypes())); //$NON-NLS-1$
		headers.put("Content-Type", Arrays.asList("application/xml")); //$NON-NLS-1$ //$NON-NLS-2$
		return headers;
	}	
	
	class ODataEntitiesResponse {
		private Feed feed;
		private String uri;
		private Iterator<Entry> rowIter;
		private String entityTypeName;
		private TranslatorException exception;
		private Status[] acceptedStatus;
		
		public ODataEntitiesResponse(String uri, Feed feed, String entityTypeName, Status... accptedStatus) {
			this.uri = uri;
			this.feed = feed;
			this.entityTypeName = entityTypeName;
			this.rowIter = this.feed.getEntries().iterator();
			this.acceptedStatus = accptedStatus;
		}
		
		public ODataEntitiesResponse(TranslatorException ex) {
			this.exception = ex;
		}
		
		public ODataEntitiesResponse() {
		}		
		
		public boolean hasRow() {
			return (this.rowIter != null && this.rowIter.hasNext());
		}
		
		public boolean hasError() {
			return exception != null;
		}
		
		public TranslatorException getError() {
			return this.exception;
		}
		
		public List<?> getNextRow(String[] columnNames, String[] embeddedColumnName, Class<?>[] expectedType) throws TranslatorException {
			if (this.rowIter != null && this.rowIter.hasNext()) {
				OEntity entity = this.rowIter.next().getEntity();
				ArrayList results = new ArrayList();
				for (int i = 0; i < columnNames.length; i++) {
					Object value = entity.getProperty(columnNames[i]).getValue();
					if (embeddedColumnName[i] != null) {
						List<OProperty<?>> embeddedProperties = (List<OProperty<?>>)value;
						for (OProperty prop:embeddedProperties) {
							if (prop.getName().equals(embeddedColumnName[i])) {
								value = prop.getValue();
								break;
							}
						}
					}
					results.add(translator.retrieveValue(value, expectedType[i]));
				}
				fetchNextBatch(!this.rowIter.hasNext());
				return results;
			}
			
			return null;
		}
		
		// TODO:there is possibility here to async execute this feed
		private void fetchNextBatch(boolean fetch) throws TranslatorException {
			if (!fetch) {
				return;
			}
			
			String next = this.feed.getNext();
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
					skip = URLDecoder.decode(skip, Charsets.Upper.UTF_8);
				} catch (UnsupportedEncodingException e) {
					throw new TranslatorException(e);
				}
				
				String nextUri = uri;
				if (uri.indexOf('?') == -1) {
					nextUri = uri + "?$skiptoken="+skip; //$NON-NLS-1$
				}
				else {
					nextUri = uri + "&$skiptoken="+skip; //$NON-NLS-1$
				}
				BinaryWSProcedureExecution execution = executeDirect("GET", nextUri, null, getDefaultHeaders()); //$NON-NLS-1$
				validateResponse(execution);
				Blob blob = (Blob)execution.getOutputParameterValues().get(0);
			    ODataVersion version = getDataServiceVersion((String)execution.getResponseHeader(ODataConstants.Headers.DATA_SERVICE_VERSION));
			    
				this.feed = parse(blob, version, this.entityTypeName); 
				this.rowIter = this.feed.getEntries().iterator();
				
			} else if (next.toLowerCase().startsWith("http")) { //$NON-NLS-1$
				BinaryWSProcedureExecution execution = executeDirect("GET", next, null, getDefaultHeaders()); //$NON-NLS-1$
				validateResponse(execution);
				Blob blob = (Blob)execution.getOutputParameterValues().get(0);
			    ODataVersion version = getDataServiceVersion((String)execution.getResponseHeader(ODataConstants.Headers.DATA_SERVICE_VERSION));
				
				this.feed = parse(blob, version, this.entityTypeName); 
				this.rowIter = this.feed.getEntries().iterator();
			} else {
				throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17001, next));
			}
		}

		private void validateResponse(BinaryWSProcedureExecution execution) throws TranslatorException {
			for (Status expected:this.acceptedStatus) {
				if (execution.getResponseCode() != expected.getStatusCode()) {
					throw buildError(execution);
				}
			}
		}
	}	
}
