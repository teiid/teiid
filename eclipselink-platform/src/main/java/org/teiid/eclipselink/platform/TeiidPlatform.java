/*
* JBoss, Home of Professional Open Source.
* See the COPYRIGHT.txt file distributed with this work for information
* regarding copyright ownership. Some portions may be licensed
* to Red Hat, Inc. under one or more contributor license agreements.
*
* This library is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License as published by the Free Software Foundation; either
* version 2.1 of the License, or (at your option) any later version.
*
* This library is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this library; if not, write to the Free Software
* Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
* 02110-1301 USA.
*/
package org.teiid.eclipselink.platform;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Hashtable;

import org.eclipse.persistence.expressions.ExpressionOperator;
import org.eclipse.persistence.internal.databaseaccess.DatabaseCall;
import org.eclipse.persistence.internal.databaseaccess.FieldTypeDefinition;
import org.eclipse.persistence.internal.expressions.ExpressionSQLPrinter;
import org.eclipse.persistence.internal.expressions.SQLSelectStatement;
import org.eclipse.persistence.platform.database.DatabasePlatform;
import org.eclipse.persistence.platform.database.H2Platform;
import org.eclipse.persistence.queries.ValueReadQuery;

@SuppressWarnings("nls")
public class TeiidPlatform extends DatabasePlatform{

	private static final long serialVersionUID = 6894570254643353289L;
	
	public TeiidPlatform() {
		super();
		this.pingSQL = "SELECT 1";	
		this.printOuterJoinInWhereClause = false;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected Hashtable buildFieldTypes() {
		
		Hashtable fieldTypeMapping = super.buildFieldTypes();
		
		fieldTypeMapping.put(byte[].class, new FieldTypeDefinition("varbinary", false));
		fieldTypeMapping.put(Character.class, new FieldTypeDefinition("char", false));
		fieldTypeMapping.put(Boolean.class, new FieldTypeDefinition("boolean", false));
		fieldTypeMapping.put(Byte.class, new FieldTypeDefinition("tinyint", false));
		
		fieldTypeMapping.put(Short.class, new FieldTypeDefinition("smallint", false));
		fieldTypeMapping.put(Integer.class, new FieldTypeDefinition("integer", false));
		fieldTypeMapping.put(Long.class, new FieldTypeDefinition("long", false));
		fieldTypeMapping.put(BigInteger.class, new FieldTypeDefinition("biginteger", false));
		fieldTypeMapping.put(Float.class, new FieldTypeDefinition("float", false));
		
		fieldTypeMapping.put(Double.class, new FieldTypeDefinition("double", false));
		fieldTypeMapping.put(BigDecimal.class, new FieldTypeDefinition("bigdecimal", false));
		fieldTypeMapping.put(Date.class, new FieldTypeDefinition("date", false));
		fieldTypeMapping.put(Time.class, new FieldTypeDefinition("time", false));
		fieldTypeMapping.put(Timestamp.class, new FieldTypeDefinition("timestamp", false));
		
		fieldTypeMapping.put(Object.class, new FieldTypeDefinition("object", false));
		fieldTypeMapping.put(Blob.class, new FieldTypeDefinition("blob", false));
		fieldTypeMapping.put(Clob.class, new FieldTypeDefinition("clob", false));
		fieldTypeMapping.put(SQLXML.class, new FieldTypeDefinition("xml", false));
		
		return fieldTypeMapping;
	}
	
	@Override
	public void printSQLSelectStatement(DatabaseCall call,
			ExpressionSQLPrinter printer, SQLSelectStatement statement) {
		int max = 0;
        if (statement.getQuery() != null) {
            max = statement.getQuery().getMaxRows();
        }
        if (max <= 0  || !(this.shouldUseRownumFiltering())) {
            super.printSQLSelectStatement(call, printer, statement);
            return;
        }
        statement.setUseUniqueFieldAliases(true);
        call.setFields(statement.printSQL(printer));
        printer.printString(" LIMIT ");
        printer.printParameter(DatabaseCall.MAXROW_FIELD);
        printer.printString(" OFFSET ");
        printer.printParameter(DatabaseCall.FIRSTRESULT_FIELD);
        call.setIgnoreFirstRowSetting(true);
        call.setIgnoreMaxResultsSetting(true);
	}
	
    @Override
    public int computeMaxRowsForSQL(int firstResultIndex, int maxResults){
        return maxResults - ((firstResultIndex >= 0) ? firstResultIndex : 0);
    }
	
    @Override
    public ValueReadQuery getTimestampQuery() {
    	super.getTimestampQuery();
        if (timestampQuery == null) {
            timestampQuery = new ValueReadQuery();
            timestampQuery.setSQLString("SELECT CURRENTTIMESTAMP()");
            timestampQuery.setAllowNativeSQLQuery(true);
        }
        return timestampQuery;
    }
    
    @Override
    protected void initializePlatformOperators() {
        super.initializePlatformOperators();
        //TODO: we'll need to go over all of the operators to see what isn't supported
        addOperator(ExpressionOperator.simpleFunction(ExpressionOperator.Ceil, "CEILING"));
        addOperator(H2Platform.toNumberOperator());
    }

	/**
	 * Avoid alter/create Constraint/index
	 */
	@Override
	public boolean supportsDeleteOnCascade() {
		return false;
	}

	@Override
	public boolean supportsForeignKeyConstraints() {
		return false;
	}

	@Override
	public boolean requiresUniqueConstraintCreationOnTableCreate() {
		return false;
	}
	
	@Override
	public boolean supportsIndexes() {
		return false;
	}

	@Override
	public boolean supportsLocalTempTables() {
		return true;
	}

	@Override
	public boolean supportsGlobalTempTables() {
		return false;
	}

	@Override
	public String getCreateViewString() {
		throw new RuntimeException("Teiid Server don't support create view in runtime");
	}

	@Override
	protected String getCreateTempTableSqlPrefix() {
		return "create local temporary table ";
	}


	@Override
	protected String getCreateTempTableSqlSuffix() {
		return "";
	}

}
