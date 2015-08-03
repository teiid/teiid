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

package org.teiid.translator.ws;

import java.io.StringReader;
import java.io.StringWriter;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.ws.Dispatch;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.handler.MessageContext;

import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.XMLType.Type;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;
import org.teiid.translator.WSConnection.Util;
import org.teiid.translator.ws.WSExecutionFactory.Binding;
import org.teiid.util.StAXSQLXML;

/**
 * A soap call executor - handles all styles doc/literal, rpc/encoded etc. 
 */
public class WSProcedureExecution implements ProcedureExecution {

	RuntimeMetadata metadata;
    ExecutionContext context;
    private Call procedure;
    private StAXSource returnValue;
    private WSConnection conn;
    private WSExecutionFactory executionFactory;
    
    /** 
     * @param env
     */
    public WSProcedureExecution(Call procedure, RuntimeMetadata metadata, ExecutionContext context, WSExecutionFactory executionFactory, WSConnection conn) {
        this.metadata = metadata;
        this.context = context;
        this.procedure = procedure;
        this.conn = conn;
        this.executionFactory = executionFactory;
    }
    
    public void execute() throws TranslatorException {
        List<Argument> arguments = this.procedure.getArguments();
        
        String style = (String)arguments.get(0).getArgumentValue().getValue();
        String action = (String)arguments.get(1).getArgumentValue().getValue();
        XMLType docObject = (XMLType)arguments.get(2).getArgumentValue().getValue();
        StAXSource source = null;
    	try {
	        source = convertToSource(docObject);
	        String endpoint = (String)arguments.get(3).getArgumentValue().getValue();
	        
	        if (style == null) {
	        	style = executionFactory.getDefaultBinding().getBindingId();
	        } else {
	        	try {
		        	Binding type = Binding.valueOf(style.toUpperCase());
		        	style = type.getBindingId();
	        	} catch (IllegalArgumentException e) {
	        		throw new TranslatorException(WSExecutionFactory.UTIL.getString("invalid_invocation", Arrays.toString(Binding.values()))); //$NON-NLS-1$
	        	}
	        }
	        
	        Dispatch<StAXSource> dispatch = conn.createDispatch(style, endpoint, StAXSource.class, executionFactory.getDefaultServiceMode()); 
	
			if (Binding.HTTP.getBindingId().equals(style)) {
				if (action == null) {
					action = "POST"; //$NON-NLS-1$
				}
				dispatch.getRequestContext().put(MessageContext.HTTP_REQUEST_METHOD, action);
				if (source != null && !"POST".equalsIgnoreCase(action)) { //$NON-NLS-1$
					if (this.executionFactory.getXMLParamName() == null) {
						throw new WebServiceException(WSExecutionFactory.UTIL.getString("http_usage_error")); //$NON-NLS-1$
					}
					try {
						Transformer t = TransformerFactory.newInstance().newTransformer();
						StringWriter writer = new StringWriter();
						//TODO: prevent this from being too large 
				        t.transform(source, new StreamResult(writer));
						String param = Util.httpURLEncode(this.executionFactory.getXMLParamName())+"="+Util.httpURLEncode(writer.toString()); //$NON-NLS-1$
						endpoint = WSConnection.Util.appendQueryString(endpoint, param);
					} catch (TransformerException e) {
						throw new WebServiceException(e);
					}
				}
			} else {
				if (action != null) {
					dispatch.getRequestContext().put(Dispatch.SOAPACTION_USE_PROPERTY, Boolean.TRUE);
					dispatch.getRequestContext().put(Dispatch.SOAPACTION_URI_PROPERTY, action);
				}
			}
			
			if (source == null) {
				// JBoss Native DispatchImpl throws exception when the source is null
				source = new StAXSource(XMLType.getXmlInputFactory().createXMLEventReader(new StringReader("<none/>"))); //$NON-NLS-1$
			}
			this.returnValue = dispatch.invoke(source);
		} catch (SQLException e) {
			throw new TranslatorException(e);
		} catch (WebServiceException e) {
			throw new TranslatorException(e);
		} catch (XMLStreamException e) {
			throw new TranslatorException(e);
		} finally {
			Util.closeSource(source);
		}
    }

	private StAXSource convertToSource(SQLXML xml) throws SQLException {
		if (xml == null) {
			return null;
		}
		return xml.getSource(StAXSource.class);
	}
    
    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
    	return null;
    }  
    
    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
    	Object result = returnValue;
		if (returnValue != null && procedure.getArguments().size() > 4
				&& procedure.getArguments().get(4).getDirection() == Direction.IN
				&& Boolean.TRUE.equals(procedure.getArguments().get(4).getArgumentValue().getValue())) {
			SQLXMLImpl sqlXml = new StAXSQLXML(returnValue);
			XMLType xml = new XMLType(sqlXml);
			xml.setType(Type.DOCUMENT);
			result = xml;
		}
        return Arrays.asList(result);
    }    
    
    public void close() {
    	
    }

    public void cancel() throws TranslatorException {
        // no-op
    }    
}
