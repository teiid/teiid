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
import org.teiid.language.Call;
import org.teiid.metadata.MetadataFactory;
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
	private String serviceName;
	private String portName;
    
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
    
    private String getStyle(boolean invokeProcedure, List<Argument> arguments) {
    	if (invokeProcedure) {
    		return (String)arguments.get(0).getArgumentValue().getValue();
    	}
    	return this.procedure.getMetadataObject().getProperty(MetadataFactory.WS_URI+WSDLMetadataProcessor.BINDING, false);
    }
    
    private String getAction(boolean invokeProcedure, List<Argument> arguments) {
    	if (invokeProcedure) {
    		return (String)arguments.get(1).getArgumentValue().getValue();
    	}
    	return this.procedure.getMetadataObject().getProperty(MetadataFactory.WS_URI+WSDLMetadataProcessor.ACTION, false);
    }
    
    private XMLType getInput(boolean invokeProcedure, List<Argument> arguments) {
    	if (invokeProcedure) {
    		return (XMLType)arguments.get(2).getArgumentValue().getValue();
    	}
    	return (XMLType)arguments.get(0).getArgumentValue().getValue();
    }
    
    private String getEndpoint(boolean invokeProcedure, List<Argument> arguments) {
    	if (invokeProcedure) {
    		return (String)arguments.get(3).getArgumentValue().getValue();
    	}
    	return this.procedure.getMetadataObject().getProperty(MetadataFactory.WS_URI+WSDLMetadataProcessor.ENDPOINT, false);
    }    
    
    private boolean isStreaming() {
    	boolean invokeProcedure = this.procedure.getProcedureName().equals(WSExecutionFactory.INVOKE);
    	if (invokeProcedure) {
    		return procedure.getArguments().size() > 4 && Boolean.TRUE.equals(procedure.getArguments().get(4).getArgumentValue().getValue());
    	}
    	return false;
    }
    
	private String getXMLParameter() {
		String value = this.executionFactory.getXMLParamName();
		if (value == null) {
			value = this.procedure.getMetadataObject().getProperty(MetadataFactory.WS_URI+WSDLMetadataProcessor.XML_PARAMETER, false);
		}
		return value;
	}
	
    public void execute() throws TranslatorException {
        
        boolean invokeProcedure = this.procedure.getProcedureName().equals(WSExecutionFactory.INVOKE);
        List<Argument> arguments = this.procedure.getArguments();
        
        String style = getStyle(invokeProcedure, arguments);
        String action = getAction(invokeProcedure, arguments);
        XMLType docObject = getInput(invokeProcedure, arguments);
        
        StAXSource source = null;
    	try {
	        source = convertToSource(docObject);
	        String endpoint = getEndpoint(invokeProcedure, arguments);
	        
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
					if (this.executionFactory.getXMLParamName() == null && getXMLParameter() == null) {
						throw new WebServiceException(WSExecutionFactory.UTIL.getString("http_usage_error")); //$NON-NLS-1$
					}
					try {
						Transformer t = TransformerFactory.newInstance().newTransformer();
						StringWriter writer = new StringWriter();
						//TODO: prevent this from being too large 
				        t.transform(source, new StreamResult(writer));
						String param = Util.httpURLEncode(getXMLParameter())+"="+Util.httpURLEncode(writer.toString()); //$NON-NLS-1$
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
    	if (returnValue != null && isStreaming()) {
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
    
	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getPortName() {
		return portName;
	}

	public void setPortName(String portName) {
		this.portName = portName;
	}	    
}
