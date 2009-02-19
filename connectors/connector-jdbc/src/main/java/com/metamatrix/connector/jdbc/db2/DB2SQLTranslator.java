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

/*
 */
package com.metamatrix.connector.jdbc.db2;

import java.util.Arrays;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.SourceSystemFunctions;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.jdbc.extension.SQLTranslator;
import com.metamatrix.connector.jdbc.extension.impl.AliasModifier;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.IJoin;
import com.metamatrix.connector.language.ILimit;
import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.connector.language.ICompareCriteria.Operator;
import com.metamatrix.connector.language.IJoin.JoinType;
import com.metamatrix.connector.visitor.framework.HierarchyVisitor;

/**
 */
public class DB2SQLTranslator extends SQLTranslator {

	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		super.initialize(env);
        registerFunctionModifier(SourceSystemFunctions.CONVERT, new DB2ConvertModifier(getLanguageFactory())); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("day")); //$NON-NLS-1$ //$NON-NLS-2$        
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$ //$NON-NLS-2$
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr")); //$NON-NLS-1$ //$NON-NLS-2$
    }
	
	@Override
	public String addLimitString(String queryCommand, ILimit limit) {
		return queryCommand + " FETCH FIRST " + limit.getRowLimit() + " ROWS ONLY"; //$NON-NLS-1$
	}
	
	@Override
	public ICommand modifyCommand(ICommand command, ExecutionContext context)
			throws ConnectorException {
		HierarchyVisitor hierarchyVisitor = new HierarchyVisitor() {
			@Override
			public void visit(IJoin obj) {
				if (obj.getJoinType() != JoinType.CROSS_JOIN) {
					return;
				}
				ILiteral one = getLanguageFactory().createLiteral(1, TypeFacility.RUNTIME_TYPES.INTEGER);
				obj.setCriteria(Arrays.asList(getLanguageFactory().createCompareCriteria(Operator.EQ, one, one)));
				obj.setJoinType(JoinType.INNER_JOIN);
			}
		};
		
		command.acceptVisitor(hierarchyVisitor);
		return command;
	}
	
	@Override
	public String getConnectionTestQuery() {
		return "Select 'x' from sysibm.systables where 1 = 2"; //$NON-NLS-1$
	}
    
}
