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

import org.odata4j.edm.EdmComplexType;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmFunctionImport;
import org.odata4j.edm.EdmSchema;
import org.odata4j.edm.EdmType;
import org.odata4j.internal.EdmDataServicesDecorator;

public class TeiidEdmMetadata extends EdmDataServicesDecorator {
	private EdmDataServices delegate;
	private String schemaName;
	
	public TeiidEdmMetadata(String schemaName, EdmDataServices delegate) {
		this.schemaName = schemaName;
		this.delegate = delegate;
	}
	
	@Override
	protected EdmDataServices getDelegate() {
		return this.delegate;
	}

	@Override
	public EdmType findEdmEntityType(String name) {
		return super.findEdmEntityType(teiidSchemaBasedName(name));
	}

	private String teiidSchemaBasedName(String name) {
		return this.schemaName+"."+name; //$NON-NLS-1$
	}
	
	@Override
	public EdmEntitySet findEdmEntitySet(String name) {
		return super.findEdmEntitySet(teiidSchemaBasedName(name));
	}
	
	@Override
	public EdmComplexType findEdmComplexType(String complexTypeFQName) {
		return super.findEdmComplexType(teiidSchemaBasedName(complexTypeFQName));
	}

	@Override
	public EdmFunctionImport findEdmFunctionImport(String functionImportName) {
		return super.findEdmFunctionImport(teiidSchemaBasedName(functionImportName));
	}
	
	@Override
	public EdmSchema findSchema(String namespace) {
		return super.findSchema(this.schemaName);
	}
}