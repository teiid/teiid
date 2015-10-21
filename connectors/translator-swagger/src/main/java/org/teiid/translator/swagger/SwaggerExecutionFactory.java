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

package org.teiid.translator.swagger;

import java.util.Collections;
import java.util.List;

import javax.resource.cci.ConnectionFactory;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.http.HTTPBinding;
import javax.xml.ws.soap.SOAPBinding;

import org.teiid.core.BundleUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.Call;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.WSConnection;

@Translator(name="swagger", description="A translator for making swagger based data service call")
public class SwaggerExecutionFactory extends ExecutionFactory<ConnectionFactory, WSConnection> {

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
		
	private Mode defaultServiceMode = Mode.PAYLOAD;
	private Binding defaultBinding = Binding.SOAP12;
	private String xmlParamName;
	
	public SwaggerExecutionFactory() {
		setSourceRequiredForMetadata(true);
	}

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
	
	@TranslatorProperty(description="Used with the HTTP binding (typically with the GET method) to indicate that the request document should be part of the query string.", display="XML Param Name")
	public String getXMLParamName() {
		return xmlParamName;
	}
	
	public void setXMLParamName(String xmlParamName) {
		this.xmlParamName = xmlParamName;
	}
	
    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, WSConnection connection) throws TranslatorException {
    
    	return new SwaggerProcedureExecution(command, metadata, executionContext, this, connection);
    }
    
	@Override
    public final List<String> getSupportedFunctions() {
        return Collections.emptyList();
    }
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory, WSConnection conn) throws TranslatorException {
		
		if(conn != null && conn.getSwagger() != null) {
		    SwaggerMetadataProcessor metadataProcessor = new SwaggerMetadataProcessor(conn.getSwagger());
		    PropertiesUtils.setBeanProperties(metadataProcessor, metadataFactory.getModelProperties(), "importer"); //$NON-NLS-1$
		    metadataProcessor.process(metadataFactory, conn);
		}
		
	}
	
	@Override
	public boolean areLobsUsableAfterClose() {
		return true;
	}

}
