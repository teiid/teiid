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

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;
import org.teiid.metadata.FunctionMethod;

/**
 * Represents a function.  A function has a name and 0..n 
 * Expressions that are parameters.  
 */
public class Function extends BaseLanguageObject implements Expression, MetadataReference<FunctionMethod> {

    private String name;
    private List<Expression> parameters;
    private Class<?> type;
    private FunctionMethod metadataObject;
    
    public Function(String name, List<? extends Expression> params, Class<?> type) {
        this.name = name;
        if (params == null) {
        	this.parameters = new ArrayList<Expression>(0);
        } else {
        	this.parameters = new ArrayList<Expression>(params);
        }
        this.type = type;
    }
    
    @Override
    public FunctionMethod getMetadataObject() {
    	return metadataObject;
    }
    
    public void setMetadataObject(FunctionMethod metadataObject) {
		this.metadataObject = metadataObject;
	}
    
    /**
     * Get name of the function
     * @return Function name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the parameters used in this function.
     * @return Array of IExpressions defining the parameters
     */
    public List<Expression> getParameters() {
        return parameters;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Set name of the function
     * @param name Function name
     */
    public void setName(String name) {
        this.name = name;
    }

    public Class<?> getType() {
        return this.type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

}
