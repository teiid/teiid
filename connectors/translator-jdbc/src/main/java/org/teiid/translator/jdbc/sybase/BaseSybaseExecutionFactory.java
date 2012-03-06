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

package org.teiid.translator.jdbc.sybase;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Command;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.OrderBy;
import org.teiid.language.SetQuery;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.db2.DB2ExecutionFactory;

public class BaseSybaseExecutionFactory extends JDBCExecutionFactory {
	
	@Override
    public boolean useAsInGroupAlias() {
    	return false;
    }
    
    @Override
    public boolean hasTimeType() {
    	return false;
    }
    
    @Override
    public int getTimestampNanoPrecision() {
    	return 3;
    }
    
    /**
     * SetQueries don't have a concept of TOP, an inline view is needed.
     */
    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
    	if (!(command instanceof SetQuery)) {
    		return null;
    	}
    	SetQuery queryCommand = (SetQuery)command;
		if (queryCommand.getLimit() == null) {
			return null;
    	}
		Limit limit = queryCommand.getLimit();
		OrderBy orderBy = queryCommand.getOrderBy();
		queryCommand.setLimit(null);
		queryCommand.setOrderBy(null);
		List<Object> parts = new ArrayList<Object>(6);
		parts.add("SELECT "); //$NON-NLS-1$
		parts.addAll(translateLimit(limit, context));
		parts.add(" * FROM ("); //$NON-NLS-1$
		parts.add(queryCommand);
		parts.add(") AS X"); //$NON-NLS-1$
		if (orderBy != null) {
			parts.add(" "); //$NON-NLS-1$
			parts.add(orderBy);
		}
		return parts;
    }
    
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
    	if (!supportsCrossJoin()) {
    		DB2ExecutionFactory.convertCrossJoinToInner(obj, getLanguageFactory());
    	}
    	return super.translate(obj, context);
    }
    
    protected boolean supportsCrossJoin() {
    	return false;
    }
    
    @SuppressWarnings("unchecked")
	@Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
    	return Arrays.asList("TOP ", limit.getRowLimit()); //$NON-NLS-1$
    }
    
    @Override
    public boolean useSelectLimit() {
    	return true;
    }
    
    @Override
    public Object retrieveValue(ResultSet results, int columnIndex,
    		Class<?> expectedType) throws SQLException {
    	if (expectedType == TypeFacility.RUNTIME_TYPES.BYTE) {
    		expectedType = TypeFacility.RUNTIME_TYPES.SHORT;
    	}
    	return super.retrieveValue(results, columnIndex, expectedType);
    }
    
    @Override
    public Object retrieveValue(CallableStatement results, int parameterIndex,
    		Class<?> expectedType) throws SQLException {
    	if (expectedType == TypeFacility.RUNTIME_TYPES.BYTE) {
    		expectedType = TypeFacility.RUNTIME_TYPES.SHORT;
    	}
    	return super.retrieveValue(results, parameterIndex, expectedType);
    }
    
    @Override
    public void bindValue(PreparedStatement stmt, Object param,
    		Class<?> paramType, int i) throws SQLException {
    	if (paramType == TypeFacility.RUNTIME_TYPES.BYTE) {
    		paramType = TypeFacility.RUNTIME_TYPES.SHORT;
    		param = ((Byte)param).shortValue();
    	}
    	super.bindValue(stmt, param, paramType, i);
    }
    
    public boolean nullPlusNonNullIsNull() {
    	return false;
    }
    
    public boolean booleanNullable() {
    	return false;
    }

}
