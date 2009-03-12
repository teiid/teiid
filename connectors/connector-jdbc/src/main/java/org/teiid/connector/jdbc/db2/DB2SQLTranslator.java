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

package org.teiid.connector.jdbc.db2;

import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.jdbc.translator.AliasModifier;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.IJoin;
import org.teiid.connector.language.ILimit;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.ICompareCriteria.Operator;
import org.teiid.connector.language.IJoin.JoinType;
import org.teiid.connector.visitor.framework.HierarchyVisitor;


/**
 */
public class DB2SQLTranslator extends Translator {

	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		super.initialize(env);
        registerFunctionModifier(SourceSystemFunctions.CONVERT, new DB2ConvertModifier(getLanguageFactory())); 
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("day")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr")); //$NON-NLS-1$ 
    }
		
	@SuppressWarnings("unchecked")
	@Override
	public List<?> translateLimit(ILimit limit, ExecutionContext context) {
		return Arrays.asList("FETCH FIRST ", limit.getRowLimit(), " ROWS ONLY"); //$NON-NLS-1$ //$NON-NLS-2$ 
	}
	
	@Override
	public ICommand modifyCommand(ICommand command, ExecutionContext context) {
		if (command instanceof IQuery) {
			HierarchyVisitor hierarchyVisitor = new HierarchyVisitor(false) {
				@Override
				public void visit(IJoin obj) {
					if (obj.getJoinType() != JoinType.CROSS_JOIN) {
						return;
					}
					ILiteral one = getLanguageFactory().createLiteral(1, TypeFacility.RUNTIME_TYPES.INTEGER);
					obj.getCriteria().add(getLanguageFactory().createCompareCriteria(Operator.EQ, one, one));
					obj.setJoinType(JoinType.INNER_JOIN);
				}
			};
			
			command.acceptVisitor(hierarchyVisitor);
		}
		return command;
	}
	
	@Override
	public String getDefaultConnectionTestQuery() {
		return "Select 'x' from sysibm.systables where 1 = 2"; //$NON-NLS-1$
	}
	
	@Override
	public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
		return DB2Capabilities.class;
	}
    
}
