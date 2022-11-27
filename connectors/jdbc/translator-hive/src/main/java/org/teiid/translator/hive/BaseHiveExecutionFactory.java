/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.hive;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.Insert;
import org.teiid.language.Limit;
import org.teiid.metadata.AggregateAttributes;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCMetadataProcessor;
import org.teiid.translator.jdbc.JDBCUpdateExecution;
import org.teiid.translator.jdbc.SQLConversionVisitor;

public class BaseHiveExecutionFactory extends JDBCExecutionFactory {

    protected ConvertModifier convert = new ConvertModifier();
    protected boolean useDatabaseMetaData;

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
    public String translateLiteralTime(Time timeValue) {
        if (!hasTimeType()) {
            return translateLiteralTimestamp(new Timestamp(timeValue.getTime()));
        }
        return '\'' + formatDateValue(timeValue) + '\'';
    }

    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "cast('" + formatDateValue(timestampValue) + "' as timestamp)"; //$NON-NLS-1$ //$NON-NLS-2$
    }


    @Deprecated
    @Override
    protected JDBCMetadataProcessor createMetadataProcessor() {
        return getMetadataProcessor();
    }

    @Override
    public JDBCMetadataProcessor getMetadataProcessor(){
        HiveMetadataProcessor result = new HiveMetadataProcessor();
        result.setUseDatabaseMetaData(this.useDatabaseMetaData);
        return result;
    }

    @Override
    public Object retrieveValue(ResultSet results, int columnIndex, Class<?> expectedType) throws SQLException {
        // Calendar based getX not supported by Hive
        if (expectedType.equals(Timestamp.class)) {
            return results.getTimestamp(columnIndex);
        }
        if (expectedType.equals(Date.class)) {
            return results.getDate(columnIndex);
        }
        if (expectedType.equals(Time.class)) {
            return results.getTime(columnIndex);
        }
        try {
            return super.retrieveValue(results, columnIndex, expectedType);
        } catch (SQLException e) {
            //impala for aggregate and other functions returns double, but bigdecimal is expected and the driver can't convert
            return super.retrieveValue(results, columnIndex, TypeFacility.RUNTIME_TYPES.OBJECT);
        }
    }

    @Override
    public Object retrieveValue(CallableStatement results, int parameterIndex,
            Class<?> expectedType) throws SQLException {
        // Calendar based getX not supported by Hive
        if (expectedType.equals(Timestamp.class)) {
            return results.getTimestamp(parameterIndex);
        }
        if (expectedType.equals(Date.class)) {
            return results.getDate(parameterIndex);
        }
        if (expectedType.equals(Time.class)) {
            return results.getTime(parameterIndex);
        }
        try {
            return super.retrieveValue(results, parameterIndex, expectedType);
        } catch (SQLException e) {
            //impala for aggregate and other functions returns double, but bigdecimal is expected and the driver can't convert
            return super.retrieveValue(results, parameterIndex, TypeFacility.RUNTIME_TYPES.OBJECT);
        }
    }

    @Override
    public void bindValue(PreparedStatement stmt, Object param,
            Class<?> paramType, int i) throws SQLException {
        // Calendar based setX not supported by Hive
        if (paramType.equals(Timestamp.class)) {
            stmt.setTimestamp(i, (Timestamp) param);
            return;
        }
        if (paramType.equals(Date.class)) {
            stmt.setDate(i, (Date) param);
            return;
        }
        if (paramType.equals(Time.class)) {
            stmt.setTime(i, (Time) param);
            return;
        }
        super.bindValue(stmt, param, paramType, i);
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

    @TranslatorProperty(display="Use DatabaseMetaData", description= "Use DatabaseMetaData (typical JDBC logic) for importing")
    public boolean isUseDatabaseMetaData() {
        return useDatabaseMetaData;
    }

    public void setUseDatabaseMetaData(boolean useDatabaseMetaData) {
        this.useDatabaseMetaData = useDatabaseMetaData;
    }

    public boolean requiresLeftLinearJoin() {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsLikeRegex() {
        return true;
    }

    public boolean rewriteBooleanFunctions() {
        return false;
    }

    @Override
    public boolean supportsWindowFunctionNthValue() {
        return false;
    }

}
