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
package org.teiid.translator.couchbase;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.resource.api.ConnectionFactory;

import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.ClobType.Type;
import org.teiid.core.util.Assertion;
import org.teiid.couchbase.CouchbaseConnection;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.QueryExpression;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.FunctionModifier;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.document.json.JsonValue;

@Translator(name="couchbase", description="Couchbase Translator, reads and writes the data to Couchbase")
public class CouchbaseExecutionFactory extends ExecutionFactory<ConnectionFactory, CouchbaseConnection> {

    private static final String COUCHBASE = "couchbase"; //$NON-NLS-1$

    protected Map<String, FunctionModifier> functionModifiers = new TreeMap<String, FunctionModifier>(String.CASE_INSENSITIVE_ORDER);

    private int maxBulkInsertSize = 100;
    private boolean useDouble;

    public CouchbaseExecutionFactory() {
        setSupportsSelectDistinct(true);
        setSourceRequiredForMetadata(false);
        setTransactionSupport(TransactionSupport.NONE);
        setSourceRequiredForMetadata(true);
        // Couchbase subquery returns an array every time, Join relate with use-keys-clause
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new SubstringFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.CEILING, new AliasModifier("CEIL"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("LN"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOG10, new AliasModifier("LOG"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.RAND, new AliasModifier("RANDOM"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("LOWER"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("UPPER"));//$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CONVERT, new FunctionModifier(){
            @Override
            public List<?> translate(Function function) {
                Expression param = function.getParameters().get(0);
                int targetCode = getCode(function.getType());
                if(targetCode == BYTE || targetCode == SHORT || targetCode == INTEGER || targetCode == LONG || targetCode == FLOAT || targetCode == DOUBLE || targetCode == BIGINTEGER || targetCode == BIGDECIMAL) {
                    if ((Number.class.isAssignableFrom(param.getType()))) {
                        return Arrays.asList(param);
                    }
                    return Arrays.asList("TONUMBER" + Tokens.LPAREN, param, Tokens.RPAREN);//$NON-NLS-1$
                } else if(targetCode == STRING || targetCode == CHAR) {
                    return Arrays.asList("TOSTRING" + Tokens.LPAREN, param, Tokens.RPAREN);//$NON-NLS-1$
                } else if(targetCode == BOOLEAN) {
                    return Arrays.asList("TOBOOLEAN" + Tokens.LPAREN, param, Tokens.RPAREN);//$NON-NLS-1$
                } else {
                    return Arrays.asList(param);
                }
            }});

        addPushDownFunction(COUCHBASE, "CONTAINS", TypeFacility.RUNTIME_NAMES.BOOLEAN, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "TITLE", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "LTRIM", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "TRIM", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "RTRIM", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "POSITION", TypeFacility.RUNTIME_NAMES.INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$

        addPushDownFunction(COUCHBASE, "CLOCK_MILLIS", TypeFacility.RUNTIME_NAMES.DOUBLE); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "CLOCK_STR", TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "CLOCK_STR", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "DATE_ADD_MILLIS", TypeFacility.RUNTIME_NAMES.LONG, TypeFacility.RUNTIME_NAMES.LONG, TypeFacility.RUNTIME_NAMES.INTEGER, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "DATE_ADD_STR", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.INTEGER, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "DATE_DIFF_MILLIS", TypeFacility.RUNTIME_NAMES.LONG, TypeFacility.RUNTIME_NAMES.LONG, TypeFacility.RUNTIME_NAMES.LONG, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "DATE_DIFF_STR", TypeFacility.RUNTIME_NAMES.LONG, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "DATE_PART_MILLIS", TypeFacility.RUNTIME_NAMES.INTEGER, TypeFacility.RUNTIME_NAMES.LONG, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "DATE_PART_STR", TypeFacility.RUNTIME_NAMES.INTEGER, TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "NOW_MILLIS", TypeFacility.RUNTIME_NAMES.DOUBLE); //$NON-NLS-1$
        addPushDownFunction(COUCHBASE, "NOW_STR", TypeFacility.RUNTIME_NAMES.STRING, TypeFacility.RUNTIME_NAMES.STRING); //$NON-NLS-1$
    }

    @Override
    public List<String> getSupportedFunctions() {

        List<String> supportedFunctions = new ArrayList<String>();

        // Numeric
        supportedFunctions.addAll(getDefaultSupportedFunctions());
        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.ATAN2);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.DEGREES);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.PI);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.RADIANS);
        supportedFunctions.add(SourceSystemFunctions.RAND);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.SIGN);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.TAN);

        //String
        supportedFunctions.add(SourceSystemFunctions.TAN);
        supportedFunctions.add(SourceSystemFunctions.INITCAP);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.REPEAT);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.REPLACE);

        //conversion
        supportedFunctions.add(SourceSystemFunctions.CONVERT);

        return supportedFunctions;
    }

    public void registerFunctionModifier(String name, FunctionModifier modifier) {
        this.functionModifiers.put(name, modifier);
    }

    public Map<String, FunctionModifier> getFunctionModifiers() {
        return this.functionModifiers;
    }

    public List<String> getDefaultSupportedFunctions(){
        return Arrays.asList(new String[] { "+", "-", "*", "/" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, CouchbaseConnection connection) throws TranslatorException {
        return new CouchbaseQueryExecution(this, command, executionContext, metadata, connection);
    }

    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, CouchbaseConnection connection) throws TranslatorException {
        return new CouchbaseProcedureExecution(this, command, executionContext, metadata, connection);
    }

    @Override
    public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, CouchbaseConnection connection) throws TranslatorException {
        return new CouchbaseUpdateExecution(command, this, executionContext, metadata, connection);
    }

    @Override
    public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, CouchbaseConnection connection) throws TranslatorException {
        return new CouchbaseDirectQueryExecution(arguments, command, this, executionContext, metadata, connection);
    }

    @Override
    public MetadataProcessor<CouchbaseConnection> getMetadataProcessor() {
        CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();
        mp.setUseDouble(useDouble);
        return mp;
    }

    @Override
    public boolean supportsAggregatesSum() {
        return true;
    }

    @Override
    public boolean supportsAggregatesAvg() {
        return true;
    }

    @Override
    public boolean supportsAggregatesMin() {
        return true;
    }

    @Override
    public boolean supportsAggregatesMax() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCount() {
        return true;
    }

    @Override
    public boolean supportsAggregatesCountStar() {
        return true;
    }

    @Override
    public boolean supportsAggregatesDistinct() {
        return true;
    }

    @Override
    public boolean supportsArrayAgg() {
        return true;
    }

    @Override
    public boolean supportsSelectExpression() {
        return true;
    }

    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean supportsRowOffset() {
        return true;
    }

    @Override
    public boolean supportsOrderBy() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsOrderByNullOrdering() {
        return true;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsHaving() {
        return true;
    }

    @Override
    public boolean supportsUnions() {
        return true;
    }

    @Override
    public boolean supportsIntersect() {
        return true;
    }

    @Override
    public boolean supportsExcept() {
        return true;
    }

    @Override
    public boolean supportsSelectWithoutFrom() {
        return true;
    }

    @Override
    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    public N1QLVisitor getN1QLVisitor() {
        return new N1QLVisitor(this);
    }

    public N1QLUpdateVisitor getN1QLUpdateVisitor() {
        return new N1QLUpdateVisitor(this);
    }

    public Object retrieveValue(Class<?> columnType, Object value) throws TranslatorException {

        if (value == null) {
            return null;
        }

        if (value.getClass().equals(columnType)) {
            return value;
        }

        if(columnType.equals(ClobType.class)) {
            boolean json = false;
            if (value instanceof JsonValue) {
                json = true;
            }
            ClobImpl clob = new ClobImpl(value.toString());
            ClobType result = new ClobType(clob);
            result.setType(json?Type.JSON:Type.TEXT);
            return result;
        }

        if (columnType.equals(BigInteger.class)) {
            if (value instanceof BigDecimal) {
                return ((BigDecimal)value).toBigInteger();
            }
            return BigInteger.valueOf(((Number)value).longValue());
        }

        if (columnType.equals(BigDecimal.class)) {
            if (value instanceof BigInteger) {
                value = new BigDecimal((BigInteger)value);
            } else {
                value = BigDecimal.valueOf(((Number)value).doubleValue());
            }
        }

        return value;
    }

    public void setValue(JsonObject json, String attr, Class<?> type, Object attrValue) {

        if(type.equals(TypeFacility.RUNTIME_TYPES.STRING)) {
            json.put(attr, (String)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.INTEGER)) {
            json.put(attr, (Integer)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.LONG)) {
            json.put(attr, (Long)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.DOUBLE)) {
            json.put(attr, (Double)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.BOOLEAN)) {
            json.put(attr, (Boolean)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.BIG_INTEGER)) {
            json.put(attr, (BigInteger)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL)) {
            json.put(attr, (BigDecimal)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.NULL)) {
            json.putNull(attr);
        } else {
            json.put(attr, attrValue);
        }
    }

    public void setValue(JsonArray array, Class<?> type, Object attrValue) {

        if(type.equals(TypeFacility.RUNTIME_TYPES.STRING)) {
            array.add((String)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.INTEGER)) {
            array.add((Integer)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.LONG)) {
            array.add((Long)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.DOUBLE)) {
            array.add((Double)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.BOOLEAN)) {
            array.add((Boolean)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.BIG_INTEGER)) {
            array.add((BigInteger)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.BIG_DECIMAL)) {
            array.add((BigDecimal)attrValue);
        } else if(type.equals(TypeFacility.RUNTIME_TYPES.NULL)) {
            array.addNull();
        } else {
            array.add(attrValue);
        }
    }

    @Override
    public boolean supportsOnlyLiteralComparison() {
        return true;
    }

    @Override
    public boolean supportsUpsert() {
        return true;
    }

    @Override
    public boolean supportsBulkUpdate() {
        return true;
    }

    @TranslatorProperty(display="Max Bulk Insert Document Size", description="The max size of documents in a bulk insert. Default 100.", advanced=true)
    public int getMaxBulkInsertSize() {
        return maxBulkInsertSize;
    }

    public void setMaxBulkInsertSize(int maxBulkInsertSize) {
        Assertion.assertTrue(maxBulkInsertSize > 0, CouchbasePlugin.Util.gs(CouchbasePlugin.Event.TEIID29020));
        this.maxBulkInsertSize = maxBulkInsertSize;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        //support only the known types
        if (toType == FunctionModifier.STRING || toType == FunctionModifier.INTEGER
            || toType == FunctionModifier.DOUBLE || toType == FunctionModifier.BOOLEAN || toType == FunctionModifier.OBJECT
            || !useDouble && (toType == FunctionModifier.LONG || toType == FunctionModifier.BIGINTEGER
                || toType == FunctionModifier.BIGDECIMAL)) {
            return super.supportsConvert(fromType, toType);
        }
        return false;
    }

    @Override
    public NullOrder getDefaultNullOrder() {
        return NullOrder.LOW;
    }

    @Override
    public boolean supportsSearchedCaseExpressions() {
        return true;
    }

    @Override
    public boolean returnsSingleUpdateCount() {
        return true;
    }

    @TranslatorProperty(display="Use Double", description="Use double rather than allowing for more precise types, such as long, bigdecimal, and biginteger", advanced=true)
    public boolean isUseDouble() {
        return useDouble;
    }

    public void setUseDouble(boolean useDouble) {
        this.useDouble = useDouble;
    }

}
