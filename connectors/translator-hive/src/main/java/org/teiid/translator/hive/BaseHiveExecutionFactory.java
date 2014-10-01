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
package org.teiid.translator.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.Insert;
import org.teiid.language.Limit;
import org.teiid.metadata.AggregateAttributes;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;
import org.teiid.translator.jdbc.JDBCUpdateExecution;
import org.teiid.translator.jdbc.SQLConversionVisitor;

public class BaseHiveExecutionFactory extends JDBCExecutionFactory {
	
	protected ConvertModifier convert = new ConvertModifier();

    @Override
    public JDBCUpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
            throws TranslatorException {
        if (command instanceof Insert) {
            return new JDBCUpdateExecution(command, conn, executionContext, this);
        }
        throw new TranslatorException(HivePlugin.Event.TEIID24000, HivePlugin.Util.gs(HivePlugin.Event.TEIID24000, command));
    }	
    
    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
            throws TranslatorException {
        throw new TranslatorException(HivePlugin.Event.TEIID24000, HivePlugin.Util.gs(HivePlugin.Event.TEIID24000, command));
    }    
    
    @Override
    public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection conn)
            throws TranslatorException {
        throw new TranslatorException(HivePlugin.Event.TEIID24000, HivePlugin.Util.gs(HivePlugin.Event.TEIID24000, command));
    }    

	@Override
    public SQLConversionVisitor getSQLConversionVisitor() {
    	return new HiveSQLConversionVisitor(this);
    }

	@Override
    public boolean useAnsiJoin() {
    	return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
    	//https://issues.apache.org/jira/browse/HIVE-784
        return false;
    }

    @Override
    public boolean supportsExistsCriteria() {
        return false;
    }

    @Override
    public boolean supportsInCriteriaSubquery() {
    	// the website documents a way to semi-join to re-write this but did not handle NOT IN case.
        return false;
    }

    @Override
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return false;
    }

    @Override
    public boolean supportsBulkUpdate() {
    	return false;
    }

    @Override
    public boolean supportsBatchedUpdates() {
    	return false;
    }

    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
    	return null;
    }

    @Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
    	return null;
    }

    @Override
    public boolean addSourceComment() {
        return false;
    }

    @Override
    public boolean useAsInGroupAlias(){
        return false;
    }

    @Override
	public boolean hasTimeType() {
    	return false;
    }

	@Override
	public String getLikeRegexString() {
		return "REGEXP"; //$NON-NLS-1$
	}

    @Override
    public boolean supportsScalarSubqueries() {
    	// Supported only in FROM clause
        return false;
    }

    @Override
    public boolean supportsInlineViews() {
    	// must be aliased.
        return true;
    }

    @Override
    public boolean supportsUnions() {
        return true;
        // only union all in subquery
    }

    @Override
    public boolean supportsInsertWithQueryExpression() {
    	return false; // insert seems to be only with overwrite always
    }

    @Override
    public boolean supportsIntersect() {
    	return false;
    }

    @Override
    public boolean supportsExcept() {
    	return false;
    }

    @Override
    public boolean supportsCommonTableExpressions() {
    	return false;
    }
    
	@Override
	public boolean supportsRowLimit() {
		return true;
	}
	

    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "true"; //$NON-NLS-1$
        }
        return "false"; //$NON-NLS-1$
    }

    @Override
    public String translateLiteralDate(java.sql.Date dateValue) {
        return '\'' + formatDateValue(dateValue) + '\'';
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
    	if (!hasTimeType()) {
    		return translateLiteralTimestamp(new Timestamp(timeValue.getTime()));
    	}
    	return '\'' + formatDateValue(timeValue) + '\'';
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return '\'' + formatDateValue(timestampValue) + '\'';
    }


    @Deprecated
    @Override
    protected JDBCMetdataProcessor createMetadataProcessor() {
        return (HiveMetadataProcessor)getMetadataProcessor();
    }
    
    @Override
    public MetadataProcessor<Connection> getMetadataProcessor(){
        return new HiveMetadataProcessor();
    }

    @Override
    public Object retrieveValue(ResultSet results, int columnIndex, Class<?> expectedType) throws SQLException {
    	if (expectedType.equals(Timestamp.class)) {
    		// Calendar based getTimestamp not supported by Hive
    		return results.getTimestamp(columnIndex);
    	}
    	return super.retrieveValue(results, columnIndex, expectedType);
    }
    
    protected FunctionMethod addAggregatePushDownFunction(String qualifier, String name, String returnType, String...paramTypes) {
    	FunctionMethod method = addPushDownFunction(qualifier, name, returnType, paramTypes);
    	AggregateAttributes attr = new AggregateAttributes();
    	attr.setAnalytic(true);
    	method.setAggregateAttributes(attr);
    	return method;
    }
    
    @Override
    public boolean supportsHaving() {
    	return false; //only having with group by
    }
    
    @Override
    public boolean supportsConvert(int fromType, int toType) {
    	if (!super.supportsConvert(fromType, toType)) {
    		return false;
    	}
    	if (convert.hasTypeMapping(toType)) {
    		return true;
    	}
    	return false;
    }
}
