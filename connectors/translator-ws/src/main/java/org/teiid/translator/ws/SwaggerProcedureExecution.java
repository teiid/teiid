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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.activation.DataSource;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;

public class SwaggerProcedureExecution implements ProcedureExecution{
    
    RuntimeMetadata metadata;
    ExecutionContext context;
    private Call procedure;
    private DataSource returnValue;
    private WSConnection conn;
    WSExecutionFactory executionFactory;
    Map<String, List<String>> customHeaders;
    Map<String, Object> responseContext = Collections.emptyMap();
    int responseCode = 200;
    private boolean useResponseContext;

    public SwaggerProcedureExecution(Call procedure, RuntimeMetadata metadata, ExecutionContext context, WSExecutionFactory executionFactory, WSConnection conn) {
        this.metadata = metadata;
        this.context = context;
        this.procedure = procedure;
        this.conn = conn;
    }

    @Override
    public void execute() throws TranslatorException {
        
        List<Argument> arguments = this.procedure.getArguments();
        for(Argument argument : arguments){
            ProcedureParameter parameter = argument.getMetadataObject();
            System.out.println(parameter.getProperties());
            System.out.println(argument.getArgumentValue().getValue());
        }
        
        System.out.println(arguments);
        
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void cancel() throws TranslatorException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        // TODO Auto-generated method stub
        return null;
    }


}
