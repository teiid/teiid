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

package org.teiid.translator.xml;

import java.sql.SQLXML;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.cci.ConnectionFactory;
import javax.xml.transform.Source;
import javax.xml.ws.Dispatch;

import org.teiid.language.Call;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.FileConnection;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.xml.streaming.BaseStreamingExecution;

public class XMLExecutionFactory extends ExecutionFactory {
		
	private String saxFilterProviderClass;
	private String encoding = "ISO-8859-1"; //$NON-NLS-1$
	private Map<String, SQLXML> responses = new ConcurrentHashMap<String, SQLXML>();
	private boolean logRequestResponseDocs = false;
	private String queryPreprocessorClass;

	@TranslatorProperty(description="Encoding of the XML documents", display="Encoding Scheme")
	public String getCharacterEncodingScheme() {
		return this.encoding;
	}
	
	public void setCharacterEncodingScheme(String encoding) {
		this.encoding = encoding;
	}
	
	@TranslatorProperty(description="Must be extension of org.teiid.translator.xml.SAXFilterProvider class", display="SAX Filter Provider Class")
	public String getSaxFilterProviderClass() {
		return this.saxFilterProviderClass;
	}

	public void setSaxFilterProviderClass(String saxFilterProviderClass) {
		this.saxFilterProviderClass = saxFilterProviderClass;
	}

	@TranslatorProperty(display="Request Preprocessor Class", description="Must be extension of org.teiid.translator.xml.RequestPreprocessor")
	public String getRequestPreprocessorClass() {
		return this.queryPreprocessorClass;
	}

	public void setRequestPreprocessorClass(String queryPreprocessorClass) {
		this.queryPreprocessorClass = queryPreprocessorClass;
	}

	// Can we get rid of this?
	private String inputStreamFilterClass;

	public String getInputStreamFilterClass() {
		return this.inputStreamFilterClass;
	}

	public void setInputStreamFilterClass(String inputStreamFilterClass) {
		this.inputStreamFilterClass = inputStreamFilterClass;
	}

	@TranslatorProperty(description="Log the XML request/response documents", display="Log Request/Response Documents")
	public boolean isLogRequestResponseDocs() {
		return logRequestResponseDocs && LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL);
	}

	public void setLogRequestResponseDocs(Boolean logRequestResponseDocs) {
		this.logRequestResponseDocs = logRequestResponseDocs;
	}

	public SQLXML getResponse(String key) {
		return this.responses.get(key);
	}
	
	public void setResponse(String key, SQLXML xml) {
		this.responses.put(key, xml);
	}
	
	public SQLXML removeResponse(String key) {
		return this.responses.remove(key);
	}	
	
	
    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
    		throws TranslatorException {
    	try {
			ConnectionFactory cf = (ConnectionFactory)connectionFactory;
			Connection connection = cf.getConnection();
			if (connection instanceof FileConnection) {
				return new FileProcedureExecution(command, this, (FileConnection)connection);
			}
			else if (connection instanceof Dispatch<?>) {
				return new XMLProcedureExecution(command, metadata, executionContext, this, (Dispatch)connection);
			}
			else {
				return null;
			}
			
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}
    }
    
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
			throws TranslatorException {
    	RequestPreprocessor preProcessor = getRequestPreProcessor();
    	if (preProcessor != null) {
    		command = preProcessor.preprocessQuery((Select)command, metadata, executionContext);
    	}
    	
		try {
			ConnectionFactory cf = (ConnectionFactory)connectionFactory;
			Connection connection = cf.getConnection();
			if (connection instanceof FileConnection) {
				return new FileResultSetExecution((Select)command, this, (FileConnection)connection);
			}
			else if (connection instanceof Dispatch<?>) {
				return new BaseStreamingExecution((Select)command, metadata, executionContext, this, (Dispatch)connection);				
			}
			return null;
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}
	}	
	
	private RequestPreprocessor getRequestPreProcessor() throws TranslatorException {
		String className = getRequestPreprocessorClass();
		if (className == null) {
			return null;
		}
		return getInstance(RequestPreprocessor.class, className, null, null);
	}
	
	public SAXFilterProvider getSaxFilterProvider() throws TranslatorException {
		if (getSaxFilterProviderClass() == null) {
			return null;
		}
		return getInstance(SAXFilterProvider.class, getSaxFilterProviderClass(), null, null);
	}  	
	
    public SQLXML convertToXMLType(Source value) {
    	return (SQLXML)getTypeFacility().convertToRuntimeType(value);
    } 	
	
	@Override
    public final int getMaxInCriteriaSize() {
    	return Integer.MAX_VALUE;
    }
	
	@Override
    public final List getSupportedFunctions() {
        return Collections.EMPTY_LIST;
    }

	@Override
    public final boolean supportsCompareCriteriaEquals() {
        return true;
    }

	@Override
    public final boolean supportsInCriteria() {
        return true;
    }
}
