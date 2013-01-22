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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.*;

import org.odata4j.core.*;
import org.odata4j.core.ODataConstants.Charsets;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.format.*;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.language.*;
import org.teiid.language.Argument.Direction;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

public class ODataQueryExecution implements ResultSetExecution {
    
	private WSConnection connection;
	private ODataExecutionFactory translator;
	private RuntimeMetadata metadata;
	private ExecutionContext executionContext;
	private ODataEntitiesResponse response;
	private EdmDataServices edsMetadata;
	private ODataSQLVisitor visitor;
	private int countResponse = -1;
	private Class<?>[] expectedColumnTypes;
	private String[] columnNames;
	private String[] embeddedColumnNames;
	
	public ODataQueryExecution(ODataExecutionFactory translator,
			QueryExpression command, ExecutionContext executionContext,
			RuntimeMetadata metadata, WSConnection connection, EdmDataServices edsMetadata) throws TranslatorException {
		
		this.metadata = metadata;
		this.executionContext = executionContext;
		this.translator = translator;
		this.connection = connection;
		this.edsMetadata = edsMetadata;
		
		this.visitor = new ODataSQLVisitor(this.translator, metadata);
    	this.visitor.visitNode(command);
    	if (!this.visitor.exceptions.isEmpty()) {
    		throw visitor.exceptions.get(0);
    	}  
    	this.expectedColumnTypes = command.getColumnTypes();
    	int colCount = this.visitor.getSelect().length;
    	this.columnNames = new String[colCount];
    	this.embeddedColumnNames = new String[colCount];
    	for (int i = 0; i < colCount; i++) {
    		Column column = this.visitor.getSelect()[i];
    		this.columnNames[i] = column.getName();
    		this.embeddedColumnNames[i] = null;
    		String entityType = column.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false);
    		if (entityType != null) {
    			String parentEntityType = column.getParent().getProperty(ODataMetadataProcessor.ENTITY_TYPE, false);
    			if (!parentEntityType.equals(entityType)) {
    				// this is embedded column
    				this.columnNames[i] = entityType;
    				this.embeddedColumnNames[i] = column.getNameInSource();
    			}
    		}
    	}
	}

	@Override
	public void close() {

	}

	@Override
	public void cancel() throws TranslatorException {

	}

	@Override
	public void execute() throws TranslatorException {
		String URI = this.visitor.buildURL();
		
		String[] headers = new String[] {"text/xml", "text/plain"}; //$NON-NLS-1$ //$NON-NLS-2$
		if (!this.visitor.isCount()) {
			headers = FormatType.ATOM.getAcceptableMediaTypes();
		}
		
		BinaryWSProcedureExecution execution = executeRequest(URI, headers);
		Blob blob = (Blob)execution.getOutputParameterValues().get(0);
		ODataVersion version = getDataServiceVersion((String)execution.getResponseHeader(ODataConstants.Headers.DATA_SERVICE_VERSION));				
		
		if (this.visitor.isCount()) {
			try {
				this.countResponse = Integer.parseInt(ObjectConverterUtil.convertToString(blob.getBinaryStream()));
			} catch (IOException e) {
				throw new TranslatorException(e);
			} catch (SQLException e) {
				throw new TranslatorException(e);
			}
		}
		else {
			Feed feed = parse(blob, version);
			this.response = new ODataEntitiesResponse(URI, feed);
		}
	}

	private Feed parse(Blob blob, ODataVersion version) throws TranslatorException {
		try {
			// if parser is written to return raw objects; then we can avoid some un-necessary object creation
			// due to time, I am not pursuing that now.
			FormatParser<Feed> parser = FormatParserFactory.getParser(
					Feed.class, FormatType.ATOM, new Settings(version, this.edsMetadata, visitor.getEnityTable().getName(), null));
			return parser.parse(new InputStreamReader(blob.getBinaryStream()));
		} catch (SQLException e) {
			throw new TranslatorException(e);
		}			
	}
	
	private static ODataVersion getDataServiceVersion(String headerValue) {
		ODataVersion version = ODataConstants.DATA_SERVICE_VERSION;
		if (headerValue != null) {
			String[] str = headerValue.split(";"); //$NON-NLS-1$
			version = ODataVersion.parse(str[0]);
		}
		return version;
	}	
	
	private BinaryWSProcedureExecution executeRequest(String uri) throws TranslatorException {
		return executeRequest(uri, FormatType.ATOM.getAcceptableMediaTypes());
	}
	
	private BinaryWSProcedureExecution executeRequest(String uri, String[] acceptHeaders) throws TranslatorException {
		try {
			System.out.println("source-url="+URLDecoder.decode(uri, "UTF-8"));
			LogManager.logDetail(LogConstants.CTX_ODATA, "Source-URL=", URLDecoder.decode(uri, "UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
		} catch (UnsupportedEncodingException e) {
		}

		List<Argument> parameters = new ArrayList<Argument>();
		parameters.add(new Argument(Direction.IN, new Literal("GET", TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null)); //$NON-NLS-1$
		parameters.add(new Argument(Direction.IN, new Literal(null, TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null));
		parameters.add(new Argument(Direction.IN, new Literal(uri, TypeFacility.RUNTIME_TYPES.STRING), TypeFacility.RUNTIME_TYPES.STRING, null));
		parameters.add(new Argument(Direction.IN, new Literal(true, TypeFacility.RUNTIME_TYPES.BOOLEAN), TypeFacility.RUNTIME_TYPES.BOOLEAN, null));

		Call call = this.translator.getLanguageFactory().createCall(ODataExecutionFactory.INVOKE_HTTP, parameters, null);
		
		BinaryWSProcedureExecution execution = new BinaryWSProcedureExecution(call, this.metadata, this.executionContext, null, this.connection);
		execution.addHeader("Content-Type", Collections.singletonList("application/xml")); //$NON-NLS-1$ //$NON-NLS-2$
		execution.addHeader("Accept", Arrays.asList(acceptHeaders)); //$NON-NLS-1$
		execution.execute();
		return execution;
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		if (visitor.isCount() && this.countResponse != -1) {
			int count = this.countResponse;
			this.countResponse = -1;
			return Arrays.asList(count);
		}

		// Feed based response
		if (this.response != null ) {
			return this.response.getNextRow(this.columnNames, this.embeddedColumnNames, this.expectedColumnTypes);
		}
		return null;
	}
	
	private class ODataEntitiesResponse {
		private Feed feed;
		private String uri;
		private Iterator<Entry> rowIter;
		
		public ODataEntitiesResponse(String uri, Feed feed) {
			this.uri = uri;
			this.feed = feed;
			this.rowIter = this.feed.getEntries().iterator();
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
				
				BinaryWSProcedureExecution execution = executeRequest(uri+"&$skiptoken="+skip); //$NON-NLS-1$
				Blob blob = (Blob)execution.getOutputParameterValues().get(0);
			    ODataVersion version = getDataServiceVersion((String)execution.getResponseHeader(ODataConstants.Headers.DATA_SERVICE_VERSION));
			    
				this.feed = parse(blob, version); 
				this.rowIter = this.feed.getEntries().iterator();
				
			} else if (next.toLowerCase().startsWith("http")) { //$NON-NLS-1$

				BinaryWSProcedureExecution execution = executeRequest(next); 
				Blob blob = (Blob)execution.getOutputParameterValues().get(0);
			    ODataVersion version = getDataServiceVersion((String)execution.getResponseHeader(ODataConstants.Headers.DATA_SERVICE_VERSION));
				
				this.feed = parse(blob, version); 
				this.rowIter = this.feed.getEntries().iterator();
			} else {
				throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17001, next));
			}
		}
	}
}
