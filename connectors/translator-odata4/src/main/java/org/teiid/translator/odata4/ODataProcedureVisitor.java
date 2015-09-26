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
package org.teiid.translator.odata4;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;

public class ODataProcedureVisitor extends HierarchyVisitor {
	protected ODataExecutionFactory executionFactory;
	protected RuntimeMetadata metadata;
	private StringBuilder buffer = new StringBuilder();
	protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

	public ODataProcedureVisitor(ODataExecutionFactory executionFactory,
			RuntimeMetadata metadata) {
		this.executionFactory = executionFactory;
		this.metadata = metadata;
	}
	
	@Override
    public void visit(Call obj) {
		this.buffer.append(obj.getProcedureName());
        
		final List<Argument> params = obj.getArguments();
        try {
            if (params != null && params.size() != 0) {
            	this.buffer.append("?"); //$NON-NLS-1$
                Argument param = null;
                for (int i = 0; i < params.size(); i++) {
                    param = params.get(i);
                    if (param.getDirection() == Direction.IN || param.getDirection() == Direction.INOUT) {
                        if (i != 0) {
                            this.buffer.append("&"); //$NON-NLS-1$
                        }
                        this.buffer.append(param.getMetadataObject().getName());
                        this.buffer.append(Tokens.EQ);
                        this.buffer.append(ODataTypeManager.convertToODataInput(param.getArgumentValue(), 
                                ODataTypeManager.odataType(param.getType()).getFullQualifiedName()
                                .getFullQualifiedNameAsString()));
                    }
                }
            }
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
	}

	public String buildURL() throws TranslatorException {
	    if (!this.exceptions.isEmpty()) {
	        throw this.exceptions.get(0);
	    }
		return this.buffer.toString();
	}
}
