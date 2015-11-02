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

import static org.teiid.translator.swagger.SwaggerPlugin.Util;

import io.swagger.models.Swagger;
import io.swagger.parser.SwaggerParser;

import java.util.Collections;
import java.util.List;

import javax.resource.cci.ConnectionFactory;

import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.Call;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;

@Translator(name="swagger", description="A translator for making swagger based data service call")
public class SwaggerExecutionFactory extends ExecutionFactory<ConnectionFactory, WSConnection> {
    
    private Swagger swagger;

	public enum Action {
		GET(Util.getString("http_get")), 
		PUT(Util.getString("http_put")),
		DELETE(Util.getString("http_delete")),
		POST(Util.getString("http_post"));
		
		private String action;
		
		private Action(String action) {
			this.action = action;
		}
		
		public String getAction() {
			return action;
		}
	}
	
	public SwaggerExecutionFactory() {
		setSourceRequiredForMetadata(true);
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
		
	    SwaggerMetadataProcessor metadataProcessor = (SwaggerMetadataProcessor) getMetadataProcessor();
	    PropertiesUtils.setBeanProperties(metadataProcessor, metadataFactory.getModelProperties(), "importer"); //$NON-NLS-1$
	    metadataProcessor.setExecutionfactory(this);
        metadataProcessor.process(metadataFactory, conn);
		
	}
	
	@Override
    public MetadataProcessor<WSConnection> getMetadataProcessor() {
        return new SwaggerMetadataProcessor();
    }

    @Override
	public boolean areLobsUsableAfterClose() {
		return true;
	}
    
    /**
     * TODO-- if 'conn.getSwagger()' will get the location of Swagger json/yaml is a http address
     *        need add logic to handle security authentication 
     * @param conn
     * @return
     * @throws TranslatorException
     */
    protected Swagger getSchema(WSConnection conn) throws TranslatorException {
        if(swagger == null) {
            String location = conn.getSwagger();
            swagger = new SwaggerParser().read(location);
        }
        return swagger;
    }
    
    protected Action getHttpAction(String action) {
        return Action.valueOf(action);
    }
    
    protected String getAction(Action action) {
        return action.getAction();
    }

}
