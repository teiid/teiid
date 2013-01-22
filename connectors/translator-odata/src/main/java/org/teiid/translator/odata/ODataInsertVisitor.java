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

import static org.teiid.language.SQLConstants.Reserved.INSERT;
import static org.teiid.language.SQLConstants.Reserved.INTO;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Insert;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;

public class ODataInsertVisitor extends HierarchyVisitor {
	protected ODataExecutionFactory executionFactory;
	protected RuntimeMetadata metadata;
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
	private StringBuilder buffer = new StringBuilder();
	private String method = "POST"; //$NON-NLS-1$
	private String entity;
	
	public ODataInsertVisitor(ODataExecutionFactory executionFactory,
			RuntimeMetadata metadata) {
		this.executionFactory = executionFactory;
		this.metadata = metadata;		
	}
	
	@Override
    public void visit(Insert obj) {
		
//		this.entity = obj.getTable().getName();
//		
//		int elementCount = obj.getColumns().size();
//		for (int i = 0; i < elementCount; i++) {
//			buffer.append(getElementName(obj.getColumns().get(i), false));
//			if (i < elementCount - 1) {
//				buffer.append(Tokens.COMMA);
//				buffer.append(Tokens.SPACE);
//			}
//		}
//
//		buffer.append(Tokens.RPAREN);
//        buffer.append(Tokens.SPACE);
//        append(obj.getValueSource());		
	}	
	
	public String toString() {
		return buffer.toString();
	}
	
	public String getURL() {
		return this.entity;
	}
	
	public String getMethod() {
		return this.method;
	}
}
