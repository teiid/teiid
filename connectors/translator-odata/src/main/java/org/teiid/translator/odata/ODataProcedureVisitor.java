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

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;

public class ODataProcedureVisitor extends HierarchyVisitor {
	protected ODataExecutionFactory executionFactory;
	protected RuntimeMetadata metadata;
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	private StringBuilder buffer = new StringBuilder();
	private String method = "GET"; //$NON-NLS-1$
	private String entityName;
	private boolean returnsTable;
	private String tableName;
	private String procedureName;
	private String returnType;
	private Class<?> returnTypeClass;
	
	public ODataProcedureVisitor(ODataExecutionFactory executionFactory,
			RuntimeMetadata metadata) {
		this.executionFactory = executionFactory;
		this.metadata = metadata;		
	}
	
	@Override
    public void visit(Call obj) {
		Procedure proc = obj.getMetadataObject();
		this.method = proc.getProperty(ODataMetadataProcessor.HTTP_METHOD, false);
		
		this.procedureName = obj.getProcedureName();
		buffer.append(obj.getProcedureName());
        final List<Argument> params = obj.getArguments();
        if (params != null && params.size() != 0) {
        	buffer.append("?"); //$NON-NLS-1$
            Argument param = null;
            for (int i = 0; i < params.size(); i++) {
                param = params.get(i);
                if (param.getDirection() == Direction.IN || param.getDirection() == Direction.INOUT) {
                    if (i != 0) {
                        buffer.append(Tokens.COMMA)
                              .append(Tokens.SPACE);
                    }
                    buffer.append(param.getMetadataObject().getName());
                    buffer.append(Tokens.EQ);
                    buffer.append(param.getArgumentValue());
                }
            }
        }
        
        // this is collection based result
        if(proc.getResultSet() != null) {
        	this.returnsTable = true;
        	this.entityName = proc.getProperty(ODataMetadataProcessor.ENTITY_TYPE, false);
        	this.tableName = proc.getFullName().substring(0, proc.getFullName().indexOf('.'))+"."+this.entityName; //$NON-NLS-1$
        }
        else {
        	for (ProcedureParameter param:proc.getParameters()) {
        		if (param.getType().equals(ProcedureParameter.Type.ReturnValue)) {
        			this.returnType = param.getRuntimeType();
        			this.returnTypeClass = param.getJavaType();
        		}
        	}
        }
	}
	
	public String buildURL() {
		return buffer.toString();
	}
	
	public String getMethod() {
		return this.method;
	}
	
	public String getEntityName() {
		return entityName;
	}	
	
	public String getTableName() {
		return this.tableName;
	}
	
	public boolean hasCollectionReturn() {
		return this.returnsTable;
	}
	
	public String getProcedureName() {
		return this.procedureName;
	}
	
	public String getReturnType() {
		return this.returnType;
	}	
	
	public Class<?>getReturnTypeClass() {
		return this.returnTypeClass;
	}	
	
}
