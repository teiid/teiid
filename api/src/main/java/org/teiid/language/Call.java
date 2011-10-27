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

package org.teiid.language;

import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;

/**
 * Represents a procedural execution (such as a stored procedure).  
 */
public class Call extends BaseLanguageObject implements Command, MetadataReference<Procedure> {

    private String name;
    private List<Argument> arguments;
    private Procedure metadataObject;
    private Class<?> returnType;
    
    public Call(String name, List<Argument> parameters, Procedure metadataObject) {
        this.name = name;
        this.arguments = parameters;
        this.metadataObject = metadataObject;
    }
    
    /**
     * Get the return type
     * @return the return parameter type or null if not expecting a return value
     */
    public Class<?> getReturnType() {
		return returnType;
	}
    
    public void setReturnType(Class<?> returnType) {
		this.returnType = returnType;
	}
    
    public String getProcedureName() {
        return this.name;
    }

    public List<Argument> getArguments() {
        return arguments;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setProcedureName(String name) {
        this.name = name;
    }

    public void setArguments(List<Argument> parameters) {
        this.arguments = parameters;
    }

    @Override
    public Procedure getMetadataObject() {
    	return this.metadataObject;
    }
    
    public ProcedureParameter getReturnParameter() {
    	for (ProcedureParameter param : this.metadataObject.getParameters()) {
    		if (param.getType() == Type.ReturnValue) {
    			return param;
    		}
    	}
    	return null;
    }

    /**
     * @return the result set types or a zero length array if no result set is returned
     */
    public Class<?>[] getResultSetColumnTypes() {
    	ColumnSet<Procedure> resultSet = this.metadataObject.getResultSet();
    	if (resultSet == null) {
    		return new Class[0];
    	}
        List<Column> columnMetadata = resultSet.getColumns();
        int size = columnMetadata.size();
        Class<?>[] coulmnDTs = new Class[size];
        for(int i =0; i<size; i++ ){
            coulmnDTs[i] = columnMetadata.get(i).getJavaType();
        }
        return coulmnDTs;
    }

}
