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

package org.teiid.metadata;

/**
 * Represents a procedure parameter
 */
public class ProcedureParameter extends BaseColumn {

	private static final long serialVersionUID = 3484281155208939073L;

	public enum Type {
		In,
		InOut,
		Out,
		ReturnValue
	}
	
	private Type type;
	private boolean optional;
	private Procedure procedure;
	private boolean isVarArg;
	
	public void setType(Type type) {
		this.type = type;
	}
	
	public Type getType() {
		return type;
	}
	
	@Deprecated
	public void setOptional(boolean optional) {
		this.optional = optional;
	}
	
	@Deprecated
	public boolean isOptional() {
		return optional;
	}
	
	public void setProcedure(Procedure procedure) {
		this.procedure = procedure;
	}
	
	@Override
	public Procedure getParent() {
		return this.procedure;
	}
	
	public void setVarArg(boolean isVarArg) {
		this.isVarArg = isVarArg;
	}

	public boolean isVarArg() {
		return isVarArg;
	}
	
    public String toString() { 
        return getName()+(isVarArg?"... ":" ")+" "+getType(); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }	
}