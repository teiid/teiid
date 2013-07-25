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

import org.teiid.language.visitor.LanguageObjectVisitor;
import org.teiid.metadata.ProcedureParameter;

public class Argument extends BaseLanguageObject implements MetadataReference<ProcedureParameter> {

	public enum Direction {
		IN,
		OUT,
		INOUT,
	}
	
    private Direction direction;
    private Expression argumentValue;
    private ProcedureParameter metadataObject;
    private Class<?> type;
    
    public Argument(Direction direction, Expression value, Class<?> type, ProcedureParameter metadataObject) {
        this.direction = direction;
        this.argumentValue = value;
    	this.metadataObject = metadataObject;
    	this.type = type;
    }
    
    /**
     * Typical constructor for an out/return parameter
     * @param direction
     * @param type
     * @param metadataObject
     */
    public Argument(Direction direction, Class<?> type, ProcedureParameter metadataObject) {
        this.direction = direction;
    	this.metadataObject = metadataObject;
    	this.type = type;
    }
    
    /**
     * Typical constructor for an in/in out parameter
     * @param direction
     * @param type
     * @param metadataObject
     */
    public Argument(Direction direction, Literal value, ProcedureParameter metadataObject) {
        this.direction = direction;
        this.argumentValue = value;
    	this.metadataObject = metadataObject;
    	if (value != null) {
    		this.type = value.getType();
    	}
    }
    
    public Direction getDirection() {
        return this.direction;
    }

    /**
     * Get the argument as a {@link Literal} value.
     * Will throw a {@link ClassCastException} if the {@link Expression} is not a {@link Literal}.
     * @return the value or null if this is an non-in parameter
     */
    public Literal getArgumentValue() {
        return (Literal)this.argumentValue;
    }
    
    public Class<?> getType() {
		return type;
	}
    
    public void setType(Class<?> type) {
		this.type = type;
	}

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public void setArgumentValue(Literal value) {
        this.argumentValue = value;
    }

    @Override
    public ProcedureParameter getMetadataObject() {
    	return this.metadataObject;
    }

    public void setMetadataObject(ProcedureParameter metadataObject) {
		this.metadataObject = metadataObject;
	}
    
    public Expression getExpression() {
    	return this.argumentValue;
    }
    
    public void setExpression(Expression ex) {
    	this.argumentValue = ex;
    }
    
}
