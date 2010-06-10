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

import java.sql.SQLXML;
import java.util.Collections;
import java.util.List;

import javax.resource.cci.ConnectionFactory;
import javax.xml.transform.Source;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.http.HTTPBinding;
import javax.xml.ws.soap.SOAPBinding;

import org.teiid.core.BundleUtil;
import org.teiid.language.Call;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.WSConnection;

@Translator(name="ws")
public class WSExecutionFactory extends ExecutionFactory<ConnectionFactory, WSConnection> {
	
	public enum Binding {
		HTTP(HTTPBinding.HTTP_BINDING), 
		SOAP11(SOAPBinding.SOAP11HTTP_BINDING),
		SOAP12(SOAPBinding.SOAP12HTTP_BINDING);
		
		private String bindingId;
		
		private Binding(String bindingId) {
			this.bindingId = bindingId;
		}
		
		public String getBindingId() {
			return bindingId;
		}
	}
	
	public static BundleUtil UTIL = BundleUtil.getBundleUtil(WSExecutionFactory.class);
		
	private Mode defaultServiceMode = Mode.PAYLOAD;
	private Binding defaultBinding = Binding.SOAP12;
	private String xmlParamName;

	@TranslatorProperty(description="Contols request/response message wrapping - set to MESSAGE for full control over SOAP messages.", display="Default Service Mode")
	public Mode getDefaultServiceMode() {
		return defaultServiceMode;
	}
	
	public void setDefaultServiceMode(Mode mode) {
		this.defaultServiceMode = mode;
	}

	@TranslatorProperty(description="Contols what SOAP or HTTP type of invocation will be used if none is specified.", display="Default Binding")
	public Binding getDefaultBinding() {
		return defaultBinding;
	}
	
	public void setDefaultBinding(Binding defaultInvocationType) {
		this.defaultBinding = defaultInvocationType;
	}
	
	public String getXmlParamName() {
		return xmlParamName;
	}
	
	public void setXmlParamName(String xmlParamName) {
		this.xmlParamName = xmlParamName;
	}
	
    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, WSConnection connection)
    		throws TranslatorException {
		return new WSProcedureExecution(command, metadata, executionContext, this, connection);
    }
    
    public SQLXML convertToXMLType(Source value) {
    	return (SQLXML)getTypeFacility().convertToRuntimeType(value);
    } 	
	
	@Override
    public final List getSupportedFunctions() {
        return Collections.EMPTY_LIST;
    }
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory,
			WSConnection conn) throws TranslatorException {
		Procedure p = metadataFactory.addProcedure("invoke"); //$NON-NLS-1$ 

		ProcedureParameter param = metadataFactory.addProcedureParameter("binding", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
		param.setAnnotation("The invocation binding (HTTP, SOAP11, SOAP12).  May be set or allowed to default to null to use the default binding.");
		param.setNullType(NullType.Nullable);

		param = metadataFactory.addProcedureParameter("action", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
		param.setAnnotation("With a SOAP invocation, action sets the SOAPAction.  With HTTP it sets the HTTP Method (GET, POST, etc.).");
		param.setNullType(NullType.Nullable);

		//can be one of string, xml, clob
		param = metadataFactory.addProcedureParameter("request", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, p); //$NON-NLS-1$
		param.setAnnotation("The String, XML, or CLOB value containing an XML document or fragment that represents the request (valid with SOAP or HTTP/POST invocations).  If the ExecutionFactory is configured in with a DefaultServiceMode or MESSAGE then SOAP request must contain the entire SOAP message.");
		param.setNullType(NullType.Nullable);
		
		param = metadataFactory.addProcedureParameter("endpoint", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
		param.setAnnotation("The relative or abolute endpoint to use.  May be set or allowed to default to null to use the default endpoint address.");
		param.setNullType(NullType.Nullable);
		
		metadataFactory.addProcedureParameter("result", TypeFacility.RUNTIME_NAMES.XML, Type.ReturnValue, p); //$NON-NLS-1$
	}

}
